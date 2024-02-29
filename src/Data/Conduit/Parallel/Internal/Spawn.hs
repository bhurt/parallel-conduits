{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Spawn
-- Description : Spawning Threads
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
module Data.Conduit.Parallel.Internal.Spawn (
    Control,
    Worker,
    spawnClient,
    spawnWorker
) where

    import qualified Control.Concurrent.Async as AsyncIO
    import           Control.Monad.Cont
    import           Control.Monad.IO.Unlift
    import qualified UnliftIO.Async           as Async

    type Control r m a = ContT r m a

    type Worker a = ContT () IO a

    spawnClient :: forall m r x .
                MonadUnliftIO m
                => m r
                -> Control x m (m r)
    spawnClient client = do
        asy <- ContT $ Async.withAsync client
        lift $ Async.link asy
        pure $ Async.wait asy

    spawnWorker :: forall m x .
                    MonadUnliftIO m
                    => Worker ()
                    -> Control x m (m ())
    spawnWorker worker = do
            asy <- ContT $ go1
            lift $ Async.link asy
            pure $ Async.wait asy
        where
            go1 :: (Async.Async () -> m x) -> m x
            go1 f = withRunInIO $ \run -> AsyncIO.withAsync task (run . f)

            task :: IO ()
            task = runContT worker pure
