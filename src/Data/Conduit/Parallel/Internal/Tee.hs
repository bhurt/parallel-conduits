{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Tee
-- Description : tee, merge, and other routing combinators.
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
module Data.Conduit.Parallel.Internal.Tee (
    tee
    , merge
) where

    import           Control.Monad.Cont
    import           Data.Conduit.Parallel.Internal.Copier
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Conduit.Parallel.Internal.Type
    import           Data.Void
    import           UnliftIO

    -- | Copy values into a sink.
    --
    -- Pictorially, @tee inner@ looks like:
    --
    -- ![image](docs/tee.svg)
    -- 
    tee :: forall r m i .
            MonadUnliftIO m
            => ParConduit m r i Void
            -> ParConduit m r i i
    tee sink = ParConduit go
        where
            go :: forall x .
                    ReadDuct i
                    -> WriteDuct i
                    -> ContT x m (m r)
            go rd wd = do
                (rd2, wd2) :: (ReadDuct i, WriteDuct i)
                    <- liftIO $ newDuct
                let wdc :: WriteDuct Void
                    (_, wdc) = newClosedDuct
                r1 :: m r <- getParConduit sink rd2 wdc
                r2 :: m () <- spawnIO $ duplicator rd wd wd2
                pure $ r2 >> r1


    -- | Merge in values from a source.
    --
    -- Pictorially, @merge source@ looks like:
    --
    -- ![image](docs/merge.svg)
    --
    merge :: forall r m o .
                MonadUnliftIO m
                => ParConduit m r () o
                -> ParConduit m r o  o
    merge source = ParConduit go
        where
            go :: forall x .
                    ReadDuct o
                    -> WriteDuct o
                    -> ContT x m (m r)
            go rd wd = do
                let crd :: ReadDuct ()
                    (crd, _) = newClosedDuct
                liftIO $ addWriteOpens wd 1
                mu :: m () <- spawnIO $ copier rd wd
                mr :: m r  <- getParConduit source crd wd
                pure $ mu >> mr

