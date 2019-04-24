{-# LANGUAGE BlockArguments #-}
module IOCache where

import           Control.Concurrent.MVar
import           Control.Monad.IO.Class

import           Data.Hashable           (Hashable)
import           Data.HashMap.Strict     (HashMap)
import qualified Data.HashMap.Strict     as HashMap

data Cache m k v = Cache
  { cacheNew        :: k -> m v
  , cacheEntries    :: MVar (HashMap k (CacheEntry v))
  , cacheFree       :: k -> v -> m ()
  , cacheGCBehavior :: GCBehavior
  , cacheGeneration :: MVar Int
  }

data CacheEntry v = CacheEntry
  { ceValue    :: v
  , ceLastRead :: Int
  }

data GCBehavior
  = NeverCollect
  | CollectOnGet Int
  | CollectExplicit Int
  deriving (Show, Eq)

newCache :: (MonadIO m)
         => (k -> m v) -- ^ acquire
         -> (k -> v -> m ()) -- ^ release
         -> GCBehavior -- ^ when and how to garbage-collect cache entries
         -> m (Cache m k v)
newCache new free gc = do
  genRef <- liftIO $ newMVar 0
  entriesRef <- liftIO $ newMVar HashMap.empty
  pure Cache
    { cacheNew = new
    , cacheEntries = entriesRef
    , cacheFree = free
    , cacheGCBehavior = gc
    , cacheGeneration = genRef
    }

cacheGet :: (MonadIO m, Hashable k, Eq k) => Cache m k v -> k -> m v
cacheGet cache k = do
  entries <- liftIO $ takeMVar (cacheEntries cache)
  let mbEntry = HashMap.lookup k entries
  curGen <- liftIO $ readMVar (cacheGeneration cache)
  newEntry <- case mbEntry of
    Nothing               -> CacheEntry <$> cacheNew cache k <*> pure curGen
    Just (CacheEntry v _) -> pure $ CacheEntry v curGen
  liftIO $ putMVar (cacheEntries cache) $ HashMap.insert k newEntry entries
  case cacheGCBehavior cache of
    CollectOnGet maxAge -> performCacheGC maxAge (cacheGeneration cache) (cacheEntries cache) (cacheFree cache)
    _ -> pure ()
  pure (ceValue newEntry)

performGC :: (MonadIO m, Hashable k, Eq k) => Cache m k v -> m ()
performGC cache = case cacheGCBehavior cache of
  CollectOnGet maxAge -> performCacheGC maxAge (cacheGeneration cache) (cacheEntries cache) (cacheFree cache)
  CollectExplicit maxAge -> performCacheGC maxAge (cacheGeneration cache) (cacheEntries cache) (cacheFree cache)
  NeverCollect -> pure ()

purge :: (MonadIO m) => Cache m k v -> m ()
purge cache = do
  entries <- liftIO $ takeMVar (cacheEntries cache)
  liftIO $ takeMVar (cacheGeneration cache)
  HashMap.traverseWithKey (\k e -> cacheFree cache k (ceValue e)) entries
  liftIO $ putMVar (cacheGeneration cache) 0
  liftIO $ putMVar (cacheEntries cache) HashMap.empty

-- | Low-level helper that does the actual work of collecting garbage in a cache
performCacheGC :: (MonadIO m, Hashable k, Eq k) => Int -> MVar Int -> MVar (HashMap k (CacheEntry v)) -> (k -> v -> m ()) -> m ()
performCacheGC maxAge curGenRef entriesRef free = do
  oldEntries <- liftIO $ takeMVar entriesRef
  curGen <- liftIO $ takeMVar curGenRef
  newEntries <- flip HashMap.traverseWithKey oldEntries \k (CacheEntry v lastRead) ->
    if curGen - lastRead > maxAge
      then Nothing <$ free k v
      else pure . Just $ CacheEntry v (lastRead - curGen)
  liftIO $ putMVar curGenRef 1
  liftIO $ putMVar entriesRef (HashMap.mapMaybe id newEntries)
