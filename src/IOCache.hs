{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE RankNTypes #-}
module IOCache
  ( -- * Core types and functions
    DCache
  , GCBehavior(..)
  , new, get, get', delete, purge, performGC
  ) where

import Data.Foldable
import Data.Functor.Compose
import Data.Functor.Identity

import           Control.Concurrent.MVar
import           Control.Monad
import           Control.Monad.IO.Class

import Data.Dependent.Map (DMap, GCompare)
import qualified Data.Dependent.Map as DMap
-- | Holds the cached data from some monadic function.
-- Create using 'new', access values using 'get'.
data DCache m k v = Cache
  { cacheCreate        :: forall a. k a -> m (v a)
  , cacheEntries    :: MVar (DMap k (CacheEntry v))
  , cacheFree       :: forall a. k a -> v a -> m ()
  , cacheGCBehavior :: GCBehavior
  , cacheGeneration :: MVar Int
  }

-- | An entry in the cache. Simply the entry's value next to the time it was
-- last accessed.
data CacheEntry v a = CacheEntry
  { ceValue    :: v a
  , ceLastRead :: Int
  }

-- | When the cache should remove old entries
data GCBehavior
  = CollectNever
  -- ^ Never remove any entries. This can cause the cache to grow very large!
  | CollectEveryNthGet Int Int
  -- ^ @CollectEveryNthGet n maxAge@ - every @n@th call to 'get',
  -- or when @'performGC'@ is called,
  -- remove entries that haven't been accessed in the last @maxAge@ generations.
  | CollectExplicitly Int
  -- ^ @CollectExplictly maxAge@ - when @'performGC'@ is called, remove
  -- entries that haven't been accessed in the last @maxAge@ generations.
  deriving (Show, Eq)

-- | Create a new cache.
new :: (MonadIO m)
    => (forall a. k a -> m (v a)) -- ^ Function to create a resource
    -> (forall a. k a -> v a -> m ()) -- ^ Function to release a resource
    -> GCBehavior -- ^ When and how to remove old entries
    -> m (DCache m k v)
new create free gc = do
  genRef <- liftIO $ newMVar 0
  entriesRef <- liftIO $ newMVar DMap.empty
  pure Cache
    { cacheCreate = create
    , cacheEntries = entriesRef
    , cacheFree = free
    , cacheGCBehavior = gc
    , cacheGeneration = genRef
    }

-- | Get a value from the cache, or create it and add it if it doesn't exist.
-- May collect old entries if the cache was configured with
-- @'CollectEveryNthGet'@.
get :: (MonadIO m, GCompare k) => DCache m k v -> k a -> m (v a)
get cache k = do
  entries <- liftIO $ takeMVar (cacheEntries cache)
  let mbEntry = DMap.lookup k entries
  curGen <- liftIO $ readMVar (cacheGeneration cache)
  newEntry <- case mbEntry of
    Nothing               -> CacheEntry <$> cacheCreate cache k <*> pure curGen
    Just (CacheEntry v _) -> pure $ CacheEntry v curGen
  liftIO $ putMVar (cacheEntries cache) $ DMap.insert k newEntry entries
  case cacheGCBehavior cache of
    CollectEveryNthGet n maxAge -> when (curGen >= n) $ performCacheGC maxAge (cacheGeneration cache) (cacheEntries cache) (cacheFree cache)
    _ -> pure ()
  pure (ceValue newEntry)

get' :: (MonadIO m, GCompare k) => DCache m k Identity -> k a -> m a
get' c = fmap runIdentity . get c

-- | Remove old entries from the cache. No effect if the cache was configures
-- with @CollectNever@.
performGC :: (MonadIO m, GCompare k) => DCache m k v -> m ()
performGC cache = case cacheGCBehavior cache of
  CollectEveryNthGet _ maxAge -> performCacheGC maxAge (cacheGeneration cache) (cacheEntries cache) (cacheFree cache)
  CollectExplicitly    maxAge -> performCacheGC maxAge (cacheGeneration cache) (cacheEntries cache) (cacheFree cache)
  CollectNever -> pure ()

-- | Remove an entry from the cache.
delete :: (MonadIO m, GCompare k) => DCache m k v -> k a -> m ()
delete cache k = do
  entries <- liftIO $ takeMVar (cacheEntries cache)
  let found = DMap.lookup k entries
  traverse_ (cacheFree cache k . ceValue) found
  liftIO $ putMVar (cacheEntries cache) (DMap.delete k entries)

-- | Remove all entries from the cache.
purge :: (MonadIO m) => DCache m k v -> m ()
purge cache = do
  entries <- liftIO $ takeMVar (cacheEntries cache)
  liftIO $ takeMVar (cacheGeneration cache)
  DMap.traverseWithKey (\k e -> e <$ cacheFree cache k (ceValue e)) entries
  liftIO $ putMVar (cacheGeneration cache) 0
  liftIO $ putMVar (cacheEntries cache) DMap.empty

-- | Low-level helper that does the actual work of collecting garbage in a cache
performCacheGC :: (MonadIO m, GCompare k) => Int -> MVar Int -> MVar (DMap k (CacheEntry v)) -> (forall a. k a -> v a -> m ()) -> m ()
performCacheGC maxAge curGenRef entriesRef free = do
  oldEntries <- liftIO $ takeMVar entriesRef
  curGen <- liftIO $ takeMVar curGenRef
  newEntries <- dmapForMaybeWithKey oldEntries \k (CacheEntry v lastRead) ->
    if curGen - lastRead > maxAge
      then Nothing <$ free k v
      else pure . Just $ CacheEntry v (lastRead - curGen)
  liftIO $ putMVar curGenRef 1
  liftIO $ putMVar entriesRef newEntries

dmapTraverseMaybeWithKey :: (Applicative f, GCompare k) => (forall a. k a -> v a -> f (Maybe (v' a))) -> DMap k v -> f (DMap k v')  
dmapTraverseMaybeWithKey f = fmap (DMap.mapMaybeWithKey (const getCompose)) . DMap.traverseWithKey (\k v -> Compose <$> f k v)

dmapForMaybeWithKey :: (Applicative f, GCompare k) => DMap k v -> (forall a. k a -> v a -> f (Maybe (v' a))) -> f (DMap k v')
dmapForMaybeWithKey dm f = dmapTraverseMaybeWithKey f dm 
