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
    spawn,
    spawnIO
) where

    import           Control.Monad.Cont      (ContT(..), lift)
    import           Control.Monad.IO.Unlift (MonadUnliftIO, liftIO)
    import qualified UnliftIO.Async          as Async

    spawn :: forall m a r .
                MonadUnliftIO m
                => m a
                -> ContT r m (m a)
    spawn act = do
            asy <- ContT $ Async.withAsync act
            lift $ Async.link asy
            pure $ Async.wait asy

    spawnIO :: forall m a r .
                MonadUnliftIO m
                => IO a
                -> ContT r m (m a)
    spawnIO = spawn . liftIO

