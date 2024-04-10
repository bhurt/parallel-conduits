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
-- This is an internal module of the Parallel Conduits library.  You
-- almost certainly want to use "Data.Conduit.Parallel"  instead.  
-- Anything in this module not explicitly re-exported by
-- "Data.Conduit.Parallel" is for internal use only, and will change
-- or disappear without notice.  Use at your own risk.
--
module Data.Conduit.Parallel.Internal.Cache (
    cache,
    cacheA
) where

    import           Data.Conduit.Parallel.Internal.Arrow   (ParArrow (..))
    import           Data.Conduit.Parallel.Internal.Control
    import           Data.Conduit.Parallel.Internal.Type
    import           Data.Conduit.Parallel.Internal.Worker
    import           Data.Void
    import           UnliftIO

    cacher :: forall m i x .
                MonadUnliftIO m
                => Int
                -> ReadDuct i
                -> WriteDuct i
                -> Control x m (m ())
    cacher size rd wd = do
            que <- makeBQueue size
            m1 :: m () <- spawnWorker $ loader que
            m2 :: m () <- spawnWorker $ unloader que
            pure $ m1 >> m2
        where
            loader :: BQueue i -> Worker ()
            loader que = do
                readi  :: Reader i <- withReadDuct rd
                writeq :: Writer i <- withWriteBQueue que
                let recur :: LoopM Void
                    recur = do
                        i <- readi
                        writeq i
                        recur
                runLoopM recur

            unloader :: BQueue i -> Worker ()
            unloader que = do
                readq  :: Reader i <- withReadBQueue que
                writei :: Writer i <- withWriteDuct wd
                let recur :: LoopM Void
                    recur = do
                        i <- readq
                        writei i
                        recur
                runLoopM recur

    -- | Create a caching ParConduit.
    --
    cache :: forall m i .  MonadUnliftIO m => Int -> ParConduit m () i i
    cache n 
        | n <= 0    = error "ParConduit.cache: size must be greater than 0!"
        | otherwise = ParConduit $ cacher n


    -- | Create a caching ParArrow
    --
    cacheA :: forall m i . MonadUnliftIO m => Int -> ParArrow m i i
    cacheA n 
        | n <= 0    = error "ParConduit.cacheA: size must be greater than 0!"
        | otherwise = ParArrow $ cacher n

