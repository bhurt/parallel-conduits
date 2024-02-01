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
    cache
) where

    import           Control.Concurrent.STM                (retry)
    import           Control.Monad.Cont
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Conduit.Parallel.Internal.Type
    import           Numeric.Natural                       (Natural)
    import           UnliftIO

    -- | Create a caching ParConduit.
    --
    cache :: forall m i .  MonadUnliftIO m => Natural -> ParConduit m () i i
    cache n = ParConduit go
        where
            go :: forall x .
                    ReadDuct i
                    -> WriteDuct i
                    -> ContT x m (m ())
            go rd wd = do
                q <- liftIO $ newTBQueueIO n
                closed <- liftIO $ newTVarIO False
                mi <- spawnIO $ loader rd q closed
                mo <- spawnIO $ unloader wd q closed
                pure $ mi >> mo

    loader :: forall i .
                ReadDuct i
                -> TBQueue i
                -> TVar Bool
                -> IO ()
    loader src q closed = withReadDuct src Nothing loop
        where
            loop :: IO (Maybe i) -> IO ()
            loop rd = do
                r <- rd
                case r of
                    Nothing -> do
                        atomically $ writeTVar closed True
                        pure ()
                    Just a -> do
                        isClosed <- atomically $ loadVar a
                        if (isClosed)
                        then pure ()
                        else loop rd

            loadVar :: i -> STM Bool
            loadVar a = do
                isClosed <- readTVar closed
                if (isClosed)
                then pure True
                else do
                    writeTBQueue q a
                    pure False

    unloader :: forall i .
                WriteDuct i
                -> TBQueue i
                -> TVar Bool
                -> IO ()
    unloader snk q closed = withWriteDuct snk Nothing loop
        where
            loop :: (i -> IO Open) -> IO ()
            loop wd = do
                mi <- atomically unloadVar
                case mi of
                    Nothing -> pure ()
                    Just i -> do
                        r <- wd i
                        case r of
                            Closed -> pure ()
                            Open   -> loop wd

            unloadVar :: STM (Maybe i)
            unloadVar = do
                mi <- tryReadTBQueue q
                case mi of
                    Just i  -> pure $ Just i
                    Nothing -> do
                        f <- readTVar closed
                        if (f)
                        then pure Nothing
                        else retry
