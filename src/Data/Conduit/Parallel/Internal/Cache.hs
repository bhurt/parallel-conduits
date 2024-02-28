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

    import           Control.Concurrent.STM                 (retry)
    import           Control.Monad.Cont
    import           Control.Monad.Trans.Maybe
    import           Data.Conduit.Parallel.Internal.Arrow   (ParArrow (..))
    import           Data.Conduit.Parallel.Internal.ParDuct
    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Conduit.Parallel.Internal.Type
    import           Data.Conduit.Parallel.Internal.Utils
    import           Data.Void
    import           Numeric.Natural                        (Natural)
    import           UnliftIO

    data Cache i = Cache {
                        queue :: TBQueue i,
                        closed :: TVar Bool
                    }

    makeCache :: forall i . Natural -> IO (Cache i)
    makeCache size = Cache
                        <$> newTBQueueIO size
                        <*> newTVarIO False

    withReadCache :: forall i . Cache i -> Worker (MaybeT IO i)
    withReadCache che = pure $ doGet
        where
            doGet :: MaybeT IO i
            doGet = MaybeT . atomically $ do
                mi <- tryReadTBQueue (queue che)
                case mi of
                    Just i  -> pure $ Just i
                    Nothing -> do
                        f <- readTVar (closed che)
                        if (f)
                        then pure Nothing
                        else retry

    withWriteCache :: forall i . Cache i -> Worker (i -> MaybeT IO ())
    withWriteCache che = ContT go
        where
            go :: ((i -> MaybeT IO ()) -> IO ()) -> IO ()
            go f = finally (f doPut) stop

            stop :: IO ()
            stop = atomically $ writeTVar (closed che) True

            doPut :: i -> MaybeT IO ()
            doPut i = MaybeT . atomically $ do
                isClosed <- readTVar (closed che)
                if (isClosed)
                then
                    pure Nothing
                else do
                    writeTBQueue (queue che) i
                    pure $ Just ()

    loader :: forall i . Cache i -> ReadDuct i -> Worker ()
    loader che rdi = do
        wc <- withWriteCache che
        ri <- withReadDuct rdi
        let recur :: MaybeT IO Void
            recur = do
                i <- readM ri
                wc i
                recur
        runM recur

    unloader :: forall i . Cache i -> WriteDuct i -> Worker ()
    unloader che wdi = do
        rc <- withReadCache che
        wi <- withWriteDuct wdi
        let recur :: MaybeT IO Void
            recur = do
                i <- rc
                writeM wi i
                recur
        runM recur

    doCache :: forall m i x .
                MonadUnliftIO m
                => Natural
                -> ReadDuct i
                -> WriteDuct i
                -> Control x m (m ())
    doCache size rdi wdi = do
        che <- liftIO $ makeCache size
        m1 <- spawnWorker $ loader che rdi
        m2 <- spawnWorker $ unloader che wdi
        pure $ m1 >> m2

    -- | Create a caching ParConduit.
    --
    cache :: forall m i .  MonadUnliftIO m => Natural -> ParConduit m () i i
    cache n = ParConduit $ doCache n


    -- | Create a caching ParArrow
    --
    cacheA :: forall m i . MonadUnliftIO m => Natural -> ParArrow m i i
    cacheA n = ParArrow $ doCache n

