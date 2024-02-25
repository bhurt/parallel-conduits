{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Cache
-- Description : A caching ParConduit
-- Copyright   : (c) Brian Hurt, 2024
-- License     : BSD 3-clause
-- Maintainer  : bhurt42@gmail.com
-- Stability   : experimental
--
-- = Warning
--
-- This is an internal module of the Parallel Conduits library.  You almost
-- certainly want to use [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- instead.  Anything in this module not explicitly re-exported 
-- by [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- is for internal use only, and will change or disappear without
-- notice.  Use at your own risk.
--
module Data.Conduit.Parallel.Internal.Cache (
    cache,
    cacheA
) where

    import           Control.Concurrent.STM               (retry)
    import qualified Control.Exception                    as Ex
    import           Control.Monad.Cont
    import           Control.Monad.Trans.Maybe
    import           Data.Conduit.Parallel.Internal.Arrow (ParArrow (..))
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Conduit.Parallel.Internal.Type
    import           Data.Conduit.Parallel.Internal.Utils
    import           Data.Void
    import           Numeric.Natural                      (Natural)
    import           UnliftIO

    data Cache i = Cache {
                        queue :: TBQueue i,
                        closed :: TVar Bool
                    }

    makeCache :: forall i . Natural -> IO (Cache i)
    makeCache size = Cache
                        <$> newTBQueueIO size
                        <*> newTVarIO False

    withCache :: forall i a . Cache i -> IO a -> IO a
    withCache che act = Ex.finally act closeCache
        where
            closeCache :: IO ()
            closeCache = atomically $ writeTVar (closed che) True

    load :: forall i . Cache i -> i -> MaybeT IO ()
    load che value =
        MaybeT . atomically $ do
            isClosed <- readTVar (closed che)
            if (isClosed)
            then
                pure Nothing
            else do
                writeTBQueue (queue che) value
                pure $ Just ()

    unload :: forall i . Cache i -> MaybeT IO i
    unload che =
        MaybeT . atomically $ do
            mi <- tryReadTBQueue (queue che)
            case mi of
                Just i  -> pure $ Just i
                Nothing -> do
                    f <- readTVar (closed che)
                    if (f)
                    then pure Nothing
                    else retry

    loader :: forall i . Cache i -> ReadDuct i -> IO ()
    loader che rdi =
        withCache che $
            withReadDuct rdi Nothing $ \ri ->
                let recur :: MaybeT IO Void
                    recur = do
                        i <- readM ri
                        load che i
                        recur
                in
                runM recur

    unloader :: forall i . Cache i -> WriteDuct i -> IO ()
    unloader che wdi =
        withCache che $
            withWriteDuct wdi Nothing $ \wi ->
                let recur :: MaybeT IO Void
                    recur = do
                        i <- unload che
                        writeM wi i
                        recur
                in
                runM recur

    doCache :: forall m i x .
                MonadUnliftIO m
                => Natural
                -> ReadDuct i
                -> WriteDuct i
                -> ContT x m (m ())
    doCache size rdi wdi = do
        che <- liftIO $ makeCache size
        m1 <- spawnIO $ loader che rdi
        m2 <- spawnIO $ unloader che wdi
        pure $ m1 >> m2

    -- | Create a caching ParConduit.
    --
    cache :: forall m i .  MonadUnliftIO m => Natural -> ParConduit m () i i
    cache n = ParConduit $ doCache n


    -- | Create a caching ParArrow
    --
    cacheA :: forall m i . MonadUnliftIO m => Natural -> ParArrow m i i
    cacheA n = ParArrow $ doCache n

