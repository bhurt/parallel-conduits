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
-- This is an internal module of the Parallel Conduits library.  You
-- almost certainly want to use "Data.Conduit.Parallel"  instead.  
-- Anything in this module not explicitly re-exported by
-- "Data.Conduit.Parallel" is for internal use only, and will change
-- or disappear without notice.  Use at your own risk.
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

    -- | The main control thread type.
    --
    -- There is one control thread, which only creates ducts and spawns
    -- worker and client threads.  All real work should be done in either
    -- a worker or client thread.  The control thread is executed by
    -- calling `Data.Conduit.Parallel.Internal.Run.runParConduit`.
    type Control r m a = ContT r m a

    -- | The worker thread type.
    --
    -- Worker threads are spawned by the control thread to do internal
    -- work- generally getting values from read ducts and writing them
    -- to write ducts.  As they do not execute client-supplied code (use
    -- client threads for that case), they do not need to execute in
    -- the client-supplied monad transformer stack.  They are thus
    -- somewhat more efficient to spawn.
    type Worker a = ContT () IO a

    -- | Spawn a client thread.
    --
    -- A client thread executes client-supplied code, and thus executes
    -- in the client-supplied monad.
    --
    -- The canonical users of this function are in
    -- "Data.Conduit.Parallel.Internal.LiftC".
    --
    spawnClient :: forall m r x .
                MonadUnliftIO m
                => m r
                -> Control x m (m r)
    spawnClient client = do
        asy <- ContT $ Async.withAsync client
        lift $ Async.link asy
        pure $ Async.wait asy

    -- | Spawn a worker thread.
    --
    -- As worker threads do not execute client-supplied code, they don't
    -- need to execute in the client-supplied monad.
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
