{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}

module Data.MapReduce where

import Prelude hiding (catch)
import Control.DeepSeq
import Control.Monad
import Control.Applicative
import Control.Exception
import Control.Monad.Reader
import Control.Concurrent (threadDelay)

import System.Environment
import System.IO

import Data.List (foldl')
import Data.Maybe
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as LB

import Data.Monoid
import qualified Data.Map as Map
import Data.Map ((!))
import Data.Binary (Binary, encode, decode)
import GHC.Exts (IsString)

import Database.Redis.Redis
import Database.Redis.Simple
import Database.Redis.ByteStringClass

import Data.CSV.Iteratee

------------------------------------------------------------------------------

newtype JobKey = JobKey { unJobKey :: ByteString }
  deriving (Show, Eq, Ord, IsString, Monoid, Binary)

type Mapper r k1 v1 = r -> (k1, [v1])

type Reducer k1 v1 = k1 -> v1 -> v1 -> v1

type Finalizer k1 v1 k2 v2 = k1 -> v1 -> (k2, v2)

data (Binary k1, Binary k2, Binary v1, Binary v2) => 
  MRSettings r k1 v1 k2 v2 = MRSettings 
    { mrRedis :: Redis
    , mrJobKey :: JobKey 
    , mrMapper :: Mapper r k1 v1
    , mrReducer :: Reducer k1 v1
    , mrFinalizer :: Finalizer k1 v1 k2 v2
    }

------------------------------------------------------------------------------
-- Feed and Mapping
------------------------------------------------------------------------------

feedCSV :: (Binary k1, Binary k2, Binary v1, Binary v2,
            NFData v1, NFData k1)
        => FilePath 
        -> CSVSettings 
        -> MRSettings MapRow k1 v1 k2 v2 
        -> IO (Either SomeException Int)
feedCSV fi csvs mrs = foldCSVFile fi csvs (feedAct mrs) 0

feedAct s = funToIterIO feed
  where
    feed i (ParsedRow (Just x)) = do
      catch (do 
              mrMap s x
              return $ i + 1)
            (\e -> do 
              let err = show (e :: ErrorCall)
              putStrLn $ "Error in mapping on row " ++ show i ++ ": " ++ err
              return $ i + 1)
    feed i _ = return i

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
            Just h' -> outputRow defCSVSettings h' (f k v') >> loop h
            Nothing -> do
              h' <- openFile fo WriteMode
              writeHeaders defCSVSettings h' ([f k v'])
              loop (Just h')
        otherwise -> loop h
  
  

------------------------------------------------------------------------------
-- Reducing
------------------------------------------------------------------------------

-- Non-terminating ongoing compaction of mapped values
mrCompactMappedAll s = do
  r <- runReaderT mrCompactMappedOne s
  case r of
    False -> do
      putStrLn "No keys left, sleeping for a while"
      threadDelay (5 * 1000 * 1000)
      mrCompactMappedAll s
    True -> mrCompactMappedAll s

-- Compact one mapped value in the mapped queue.
mrCompactMappedOne = do
  r <- asks mrRedis
  k <- liftIO $ do
    select r infoDB
    lpop r compaction_queue
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

------------------------------------------------------------------------------

mrMap s r = runReaderT f s
  where
    f = do
      mapfun <- asks mrMapper
      let (k, vs) = mapfun r
      k `deepseq` vs `deepseq` pushM k vs


pushM ::
  (Binary v2, Binary v1, Binary k2, Binary k1,
   MonadReader (MRSettings r k1 v1 k2 v2) m,
   MonadIO m) =>
  k1 -> [v1] -> m ()
pushM k vs = mapM_ push' vs
  where
    push' v = do
      r <- asks mrRedis
      j <- asks mrJobKey
      liftIO $ do
        select r mapDB
        rpush r (encode k) (encode v)
        select r infoDB
        rpush r compaction_queue (encode k)

reduceM k = do
  r <- asks mrRedis
  f <- asks mrReducer
  liftIO $ do
    lock r k
    select r lockDB 
    vals <- popList r k
    let newval = foldr1 (f (decode k)) vals
    delLock r k
    pushMapped r k newval
    

finalizeM k = do
  MRSettings r _ _ f g <- ask
  liftIO $ do
    lock r k
    select r lockDB 
    vals <- collectList r k
    let newval = foldr1 (f (decode k)) vals
    let (finkey, finval) = g (decode k) newval
    delLock r k
    select r finDB 
    set r (encode finkey) (encode finval)


mapDB = 1
lockDB = 2
finDB = 3
infoDB = 4

compaction_queue :: ByteString
compaction_queue = "mapped-compact-pending"


------------------------------------------------------------------------------
-- MapReduce related redis ops
------------------------------------------------------------------------------

popRandomFinal ::
  (Binary k, Binary v) =>
  Redis -> IO (Maybe (k, Maybe v))
popRandomFinal conn = do
  select conn finDB
  k <- randomKey conn
  case k of
    RBulk Nothing -> return Nothing
    RBulk (Just k') -> do
      v <- popFinal conn k'
      return $ Just v

popFinal conn k = do
  select conn finDB
  v <- itemGet conn (Key (B.concat . LB.toChunks $ k))
  del conn k
  return (decode k, v)

-- Lock data in the mapdb by moving to lockdb
delLock conn k = do
  select conn lockDB
  del conn k

-- Del lock in the lockdb
lock conn k = do
  select conn mapDB 
  move conn k lockDB 

-- Push a value into the mapped db
pushMapped conn k v = do
  select conn mapDB 
  rpush conn k (encode v)

-- Del value from the mapdb
delMapped conn k = do
  select conn mapDB
  del conn k

------------------------------------------------------------------------------
-- Higher level Redis primitives
------------------------------------------------------------------------------

-- Collect redis list into Haskell list
popList conn k = collect conn []
  where
    collect conn acc = do
      x <- lpop conn k
      case x of
        RBulk Nothing -> return acc
        RBulk (Just x') -> collect conn (decode x' : acc)

collectList :: (Binary a, BS s1) => Redis -> s1 -> IO [a]
collectList conn k = do
  RMulti rs <- lrange conn k (0,-1)
  del conn k
  return $ maybe [] collectVals rs
  where
    collectVals rs = reverse $ foldl' colStep [] rs
    colStep acc (RBulk Nothing) = acc
    colStep acc (RBulk (Just x)) = decode x : acc

