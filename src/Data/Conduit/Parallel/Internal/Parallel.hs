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
-- This is an internal module of the Parallel Conduits library.  You almost
-- certainly want to use [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- instead.  Anything in this module not explicitly re-exported 
-- by [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- is for internal use only, and will change or disappear without
-- notice.  Use at your own risk.
--
module Data.Conduit.Parallel.Internal.Parallel (
    parallel
) where

    import           Control.Monad.Cont
    import           Data.Conduit.Parallel.Internal.Duct
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
            go :: forall x . ReadDuct i -> WriteDuct o -> ContT x m (m r)
            go rd wd = spawn $ controlThread rd wd

            controlThread :: ReadDuct i -> WriteDuct o -> m r
            controlThread rd' wd' = flip runContT id $ do
                rd <- ContT $ commonReadClose rd'
                wd <- ContT $ commonWriteClose wd'
                let makeWorker :: ContT r m (m r)
                    makeWorker = spawn (runContT (getParConduit inner rd wd) id)
                    workers :: [ ContT r m (m r) ]
                    workers = take num (repeat makeWorker)
                rs :: [ m r ] <- sequence workers
                pure $ mconcat <$> sequence rs
                
