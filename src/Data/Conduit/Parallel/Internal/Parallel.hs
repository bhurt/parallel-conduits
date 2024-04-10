{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Parallel
-- Description : The implementation of the parallel function
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
module Data.Conduit.Parallel.Internal.Parallel (
    parallel
) where

    import           Data.Conduit.Parallel.Internal.Control
    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Conduit.Parallel.Internal.Type
    import           UnliftIO

    -- | Run multiple parconduits in parallel.
    --
    -- The code @parallel num inner@ creates a parconduit which
    -- internally runs @num@ copies of the @inner@ parconduit.  Pictorially,
    -- this looks like:
    --
    -- ![image](docs/parallel.svg)
    -- 
    -- Inputs are served to the different parconduits on an arbitrary
    -- basis, and outputs likewise will be intermingled.
    parallel :: forall m r i o .
                    (MonadUnliftIO m
                    , Monoid r)
                    => Int
                    -> ParConduit m r i o
                    -> ParConduit m r i o
    parallel num inner 
            | num <= 0  = error $ "Data.Conduit.Parallel.parallel called "
                                    ++ "with a non-positive number of threads!"
            | num == 1  = inner
            | otherwise = ParConduit go
        where
            go :: forall x . ReadDuct i -> WriteDuct o -> Control x m (m r)
            go rd wd = do
                addReadOpens rd (num - 1)
                addWriteOpens wd (num - 1)
                rs :: [ m r ] <-
                    sequence . take num . repeat $ getParConduit inner rd wd
                pure $ mconcat <$> sequence rs
