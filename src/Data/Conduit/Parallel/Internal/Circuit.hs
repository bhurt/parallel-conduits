{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Circuit
-- Description : Directing values into sub-conduits
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
module Data.Conduit.Parallel.Internal.Circuit (
    route,
    fixP,
    traverseC
) where

    import           Data.Bitraversable
    import           Data.Conduit.Parallel.Internal.Control
    import           Data.Conduit.Parallel.Internal.Copier
    import           Data.Conduit.Parallel.Internal.Type
    import           Data.Conduit.Parallel.Internal.Worker
    import           UnliftIO

    -- | Split the input into two different ParConduits.
    --
    -- ![image](docs/route.svg)
    -- 
    route :: forall m r i1 i2 o f .
                    (MonadUnliftIO m
                    , Semigroup r
                    , Bitraversable f)
                    => ParConduit m r i1 o
                    -> ParConduit m r i2 o
                    -> ParConduit m r (f i1 i2) o
    route pc1 pc2 = ParConduit go
        where
            go :: forall x .
                    ReadDuct (f i1 i2)
                    -> WriteDuct o
                    -> Control x m (m r)
            go rdf wdo = do
                (rdi1, wdi1) :: Duct i1 <- newDuct
                (rdi2, wdi2) :: Duct i2 <- newDuct
                addWriteOpens wdo 1
                mr1 <- getParConduit pc1 rdi1 wdo
                mr2 <- getParConduit pc2 rdi2 wdo
                mu  <- spawnWorker $ direct rdf wdi1 wdi2
                pure $ do
                    () <- mu
                    r1 <- mr1
                    r2 <- mr2
                    pure $ r1 <> r2

    -- | Feed outputs back in as inputs.
    --
    -- ![image](docs/fixP.svg)
    -- 
    fixP :: forall m i o r f .
                (MonadUnliftIO m
                , Bitraversable f)
                => ParConduit m r i (f i o)
                -> ParConduit m r i o
    fixP inner = ParConduit go
        where
            go :: forall x .
                    ReadDuct i
                    -> WriteDuct o
                    -> Control x m (m r)
            go rdi wro = do
                (rdf, wdf) :: Duct (f i o) <- newDuct
                (rdi', wdi') :: Duct i <- newDuct
                addWriteOpens wdi' 1
                m1 <- spawnWorker $ direct rdf wdi' wro
                m2 <- spawnWorker $ copier rdi wdi'
                mr <- getParConduit inner rdi' wdf
                pure $ m1 >> m2 >> mr

    traverseC :: forall m f a .
                    (MonadUnliftIO m
                    , Traversable f)
                    => ParConduit m () (f a) a
    traverseC = ParConduit go
        where
            go :: forall x .
                    ReadDuct (f a)
                    -> WriteDuct a
                    -> Control x m (m ())
            go rdf wda = spawnWorker $ traverser rdf wda

