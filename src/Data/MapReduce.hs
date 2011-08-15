{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}

module Data.MapReduce where

import Prelude hiding (catch)
import Control.DeepSeq
import Control.Monad
import Control.Applicative
import Control.Exception
import Control.Monad.Reader
import Control.Concurrent (threadDelay)
import Codec.Compression.Snappy.Lazy

import System.Environment
import System.IO

import Data.List (foldl')
import Data.Maybe
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as LB
import Data.Enumerator (($$))
import qualified Data.Enumerator as E
import qualified Data.Enumerator.Binary as E

import Data.Monoid
import qualified Data.Map as Map
import Data.Map ((!))
import Data.Binary (Binary, encode, decode)
import GHC.Exts (IsString)

import Database.Redis.Redis
import Database.Redis.Simple
import Database.Redis.ByteStringClass

import Data.CSV.Enumerator

------------------------------------------------------------------------------

newtype JobKey = JobKey { unJobKey :: ByteString }
  deriving (Show, Eq, Ord, IsString, Monoid, Binary)

type Mapper r k1 v1 = r -> IO (k1, [v1], Double)

type Reducer k1 v1 = k1 -> v1 -> v1 -> IO v1

type Finalizer k1 v1 k2 v2 = k1 -> v1 -> IO (k2, v2)

data (Binary k1, Binary k2, Binary v1, Binary v2) => 
  MRSettings r k1 v1 k2 v2 = MRSettings 
    { mrRedis :: Redis
    , mrJobKey :: JobKey 
    , mrMapper :: Mapper r k1 v1
    , mrReducer :: Reducer k1 v1
    , mrFinalizer :: Finalizer k1 v1 k2 v2
    }


newtype Key = Key (forall a. Binary a => a)


------------------------------------------------------------------------------
-- Feed and Mapping
------------------------------------------------------------------------------


feedCSVStream h csvs mrs = do
  hSetBinaryMode h True
  E.run iter
  where 
    iter = E.enumHandle 4096 h $$ iterCSV csvs (feedAct mrs) 0



feedCSV :: (Binary k1, Binary k2, Binary v1, Binary v2,
            NFData v1, NFData k1)
        => FilePath 
        -> CSVSettings 
        -> MRSettings MapRow k1 v1 k2 v2 
        -> IO (Either SomeException Int)
feedCSV fi csvs mrs = foldCSVFile fi csvs (feedAct mrs) 0


feedAct ::
  (CSVeable r, Binary k1, Binary v2, Binary v1, Binary k2,
   NFData k1, NFData v1, Num a) =>
  MRSettings r k1 v1 k2 v2 -> CSVAction r a
feedAct s = funToIterIO feed
  where
    feed !i (ParsedRow (Just x)) = do
      catch (do 
              mrMap s x
              return $ i + 1)
            (\e -> do 
              let err = show (e :: ErrorCall)
              putStrLn $ "Error in mapping on row " ++ show i ++ ": " ++ err
              return $ i + 1)
    feed !i _ = return i


mrMap s r = runReaderT f s
  where
    f = do
      mapfun <- asks mrMapper
      (k, vs, score) <- liftIO $ mapfun r
      k `deepseq` vs `deepseq` pushM k vs score


pushM ::
  (Binary v2, Binary v1, Binary k2, Binary k1,
   MonadReader (MRSettings r k1 v1 k2 v2) m,
   MonadIO m) =>
  k1 -> [v1] -> Double -> m ()
pushM k vs score = mapM_ push' vs >> updateScore k score >> return ()
  where
    push' v = do
      r <- asks mrRedis
      liftIO $ do
        select r mapDB
        rpush r (encode k) (enc v)


updateScore k score = do
  r <- asks mrRedis
  liftIO $ do
    select r infoDB
    zadd r scoresBuffer score (encode k)

------------------------------------------------------------------------------
-- Outputting 
------------------------------------------------------------------------------

outputCSV :: (Binary k, Binary v) 
          => Redis -> FilePath -> (k -> v -> MapRow) -> IO ()
outputCSV conn fo f = loop Nothing
  where 
    loop :: Maybe Handle -> IO ()
    loop h = do
      v <- popRandomFinal conn
      case v of
        Nothing -> do
          putStrLn "No more keys left in the finalized db. Quitting."
          maybe (return ()) hClose h
        Just (k, Just v') -> do
          case h of
            Just h' -> do
              let r = (f k v')
              outputRow defCSVSettings h' r >> loop h
            Nothing -> do
              h' <- openFile fo WriteMode
              writeHeaders defCSVSettings h' ([f k v'])
              loop (Just h')
        otherwise -> loop h
  

-- | Keep running and waiting for keys to appear in output
outputCSVCont 
  :: (Binary k, Binary v) 
  => Redis -> FilePath -> (k -> v -> MapRow) -> IO ()
outputCSVCont conn fo f = bracket acquire hClose loop
  where
    wait = do
      putStrLn "No keys in output, sleeping."
      threadDelay (10 * 1000 * 1000)
    acquire = do
      v <- popRandomFinal conn
      case v of
        Nothing -> wait >> acquire
        Just (k, Just v') -> do
          h' <- openFile fo WriteMode
          hSetBuffering h' NoBuffering
          writeHeaders defCSVSettings h' ([f k v'])
          return h'
    loop h = do
      v <- popRandomFinal conn
      case v of
        Nothing -> hFlush h >> wait >> loop h
        Just (k, Just v') -> do
          let r = (f k v')
          outputRow defCSVSettings h r >> loop h
        _ -> loop h
  

------------------------------------------------------------------------------
-- Reducing
------------------------------------------------------------------------------


mrFinalizeFinishedCont interval = do
  res <- mrFinalizeFinishedOne interval
  case res of
    False -> do
      liftIO $ do
        putStrLn "No finished keys, sleeping for a while."
        threadDelay (1000 * 1000 * 20)
      mrFinalizeFinishedCont interval
    True -> mrFinalizeFinishedCont interval


mrFinalizeFinishedOne interval = do
  r <- asks mrRedis
  k <- liftIO $ do
    select r infoDB
    atomicSortedSetPop r scoresBuffer interval
  case k of
    Just k' -> reduceM k' >> finalizeM k'
    Nothing -> return False


-- Non-terminating ongoing compaction of mapped values
mrCompactMappedAll s = do
  r <- runReaderT mrCompactMappedOne s
  case r of
    False -> do
      putStrLn "No keys in mapDB, sleeping for a while"
      threadDelay (5 * 1000 * 1000)
      mrCompactMappedAll s
    True -> mrCompactMappedAll s


-- Compact one mapped value randomly.
mrCompactMappedOne = do
  r <- asks mrRedis
  k <- liftIO $ do
    select r mapDB
    randomKey r
  case k of
    RBulk Nothing -> return False
    RBulk (Just k') -> reduceM k' >> return True


mrReduceAll s = do
  r <- runReaderT mrReduceOne s
  case r of
    False -> do
      putStrLn "All keys reduced"
    True -> mrReduceAll s


mrReduceAndFinalizeAll s = do
  r <- runReaderT mrReduceAndFinalizeOne s
  case r of
    False -> do
      putStrLn "All keys reduced and finalized"
    True -> mrReduceAndFinalizeAll s


mrReduceOne = do
  r <- asks mrRedis
  k <- liftIO $ do
    select r mapDB 
    randomKey r
  case k of
    RBulk Nothing -> return False
    RBulk (Just k') -> reduceM k' >> return True


mrReduceAndFinalizeOne = do
  r <- asks mrRedis
  liftIO $ select r mapDB 
  k <- liftIO $ randomKey r
  case k of
    RBulk Nothing -> return False
    RBulk (Just k') -> reduceM k' >> finalizeM k' >> return True


-- | Reduce, push back into mapDB
reduceM k = do
  r <- asks mrRedis
  f <- asks mrReducer
  liftIO $ do
    l <- lock r k
    case l of
      RInt 0 -> return (RInt 0)
      RInt 1 -> do
        select r lockDB 
        vals <- collectList r k
        delLock r k
        case vals of
          [] -> return (RInt 0)
          otherwise -> do
            let (v:vs) = reverse vals
            newval <- foldM (f (decode k)) v vs 
            pushMapped r k newval
        

-- Reduce, finalize and push into finDB
finalizeM k = do
  MRSettings r _ _ f g <- ask
  liftIO $ do
    l <- lock r k
    case l of
      RInt 0 -> return False
      RInt 1 -> do
        select r lockDB 
        vals <- collectList r k
        delLock r k
        case vals of
          [] -> return False
          otherwise -> do
            let (v:vs) = reverse vals
            newval <- foldM (f (decode k)) v vs 
            (finkey, finval) <- g (decode k) newval
            select r finDB 
            set r (encode finkey) (enc finval)
            return True


------------------------------------------------------------------------------
    
mapDB = 1
lockDB = 2
finDB = 3
infoDB = 4
scoresBuffer = "bucketScores" :: ByteString

------------------------------------------------------------------------------
-- MapReduce related redis ops
------------------------------------------------------------------------------

-- | Pop a random element from the finDB
popRandomFinal ::
  (Binary k, Binary v) =>
  Redis -> IO (Maybe (k, Maybe v))
popRandomFinal conn = do
  select conn finDB
  k <- randomKey conn
  case k of
    RBulk Nothing -> return Nothing
    RBulk (Just k') -> do
      kv <- popFinal conn k'
      return $ Just kv


-- | Pop an element from the finDB
popFinal conn k = do
  select conn finDB
  v <- get conn k
  del conn k
  return $ case v of
    RBulk Nothing -> (decode k, Nothing)
    RBulk (Just v') -> (decode k, Just $ dec v')


-- | Del lock in the lockdb
delLock :: Redis -> LB.ByteString -> IO (Reply Int)
delLock conn k = do
  select conn lockDB
  del conn k


-- | Lock data in the mapdb by moving to lockdb
lock :: Redis -> LB.ByteString -> IO (Reply Int)
lock conn k = do
  select conn mapDB 
  move conn k lockDB 


-- | Push a value into the mapped db
pushMapped :: (Binary v) => Redis -> LB.ByteString -> v -> IO (Reply Int)
pushMapped conn k v = do
  select conn mapDB 
  rpush conn k (enc v)


-- Del value from the mapdb
delMapped conn k = do
  select conn mapDB
  del conn k

------------------------------------------------------------------------------
-- Some useful, generic Redis operations
------------------------------------------------------------------------------


-- | Pop a single element with a socre in the given interval
atomicSortedSetPop 
  :: Redis 
  -> ByteString                 -- set buffer's name
  -> Interval Double            -- score interval to be popped
  -> IO (Maybe LB.ByteString)
atomicSortedSetPop r k interval = do
  rep <- fromRMultiBulk' =<< zrangebyscore r k interval (Just (0, 1)) False 
  case rep of
    ((a :: LB.ByteString):_) -> a `seq` zrem r k a >> return (Just a)
    _ -> return Nothing


-- Collect redis list into Haskell list; popping elements one at a time.
-- Does not delete the key.
popList conn k = collect conn []
  where
    collect conn acc = do
      x <- lpop conn k
      case x of
        RBulk Nothing -> return acc
        RBulk (Just x') -> collect conn (dec x' : acc)

-- Collect redis list into Haskell list; do a range query and delete the key.
-- Less atomic than popList, probably.
collectList :: (Binary a, BS s1) => Redis -> s1 -> IO [a]
collectList conn k = do
  RMulti rs <- lrange conn k (0,-1)
  del conn k
  return $ maybe [] collectVals rs
  where
    collectVals rs = reverse $ foldl' colStep [] rs
    colStep acc (RBulk Nothing) = acc
    colStep acc (RBulk (Just x)) = dec x : acc

-- Compression stuff

enc = compress . encode

dec = decode . decompress
