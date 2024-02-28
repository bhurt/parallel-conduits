{-# LANGUAGE InstanceSigs        #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Type
-- Description : The main parallel conduit type
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
module Data.Conduit.Parallel.Internal.Type (
    ParConduit(..),
    fuse,
    fuseLeft,
    fuseTuple,
    fuseSemigroup,
    fuseMap
) where

    import qualified Control.Category                       as Cat
    import           Control.Monad.Cont                     (ContT)
    import           Control.Monad.IO.Class
    import           Control.Monad.IO.Unlift                (MonadUnliftIO)
    import           Data.Conduit.Parallel.Internal.Copier  (copier)
    import qualified Data.Conduit.Parallel.Internal.ParDuct as Duct
    import           Data.Conduit.Parallel.Internal.Spawn
    import qualified Data.Functor.Contravariant             as Contra
    import qualified Data.Profunctor                        as Pro


    newtype ParConduit m r i o = ParConduit {
                                    getParConduit :: 
                                        forall x .
                                        Duct.ReadDuct i
                                        -> Duct.WriteDuct o
                                        -> Control x m (m r) }

    instance Functor (ParConduit m r i) where
        fmap f c = ParConduit $
                    \rd wd -> getParConduit c rd (Contra.contramap f wd)


    instance Pro.Profunctor (ParConduit m r) where
        dimap f g c = ParConduit $
                        \rd wd ->
                            getParConduit c
                                (fmap f rd)
                                (Contra.contramap g wd)
        lmap f c = ParConduit $
                    \rd wd ->
                        getParConduit c
                            (fmap f rd)
                            wd

        rmap g c = ParConduit $
                    \rd wd ->
                        getParConduit c
                            rd
                            (Contra.contramap g wd)
    

    -- | Fuse two conduits, with a function to combine the results.
    --
    -- ![image](docs/fuseMap.svg)
    --
    fuseMap :: forall r1 r2 r m i o x .
                    (MonadIO m)
                    => (r1 -> r2 -> r)
                    -> ParConduit m r1 i x
                    -> ParConduit m r2 x o
                    -> ParConduit m r i o
    fuseMap fixr pc1 pc2 = ParConduit go
        where
            go :: forall t .
                    Duct.ReadDuct i
                    -> Duct.WriteDuct o
                    -> ContT t m (m r)
            go rd wd = do
                (xrd, xwd) :: Duct.Duct x <- Duct.newDuct
                r1 <- getParConduit pc1 rd xwd
                r2 <- getParConduit pc2 xrd wd
                pure $ fixr <$> r1 <*> r2


    instance (MonadUnliftIO m, Monoid r) => Cat.Category (ParConduit m r) where
        id :: forall a . ParConduit m r a a
        id = ParConduit go
                where
                    go :: Duct.ReadDuct a
                            -> Duct.WriteDuct a
                            -> ContT t m (m r)
                    go rd wd = do
                        r :: m () <- spawnWorker $ copier rd wd
                        pure (mempty <$ r)

        (.) = flip $ fuseMap mappend

    -- | Fuse two parallel conduits, returning the second result.
    --
    -- ![image](docs/fuse.svg)
    --
    fuse :: forall r m i o x .
                    MonadIO m
                    => ParConduit m () i x
                    -> ParConduit m r x o
                    -> ParConduit m r i o
    fuse = fuseMap (\() r -> r)

    -- | Fuse two parallel conduits, returning the first result.
    --
    -- ![image](docs/fuseLeft.svg)
    --
    fuseLeft :: forall r m i o x .
                    MonadIO m
                    => ParConduit m r i x
                    -> ParConduit m () x o
                    -> ParConduit m r i o
    fuseLeft = fuseMap (\r () -> r)

    -- | Fuse two parallel conduits, constructing a tuples of the results.
    --
    -- ![image](docs/fuseTuple.svg)
    --
    fuseTuple :: forall r1 r2 m i o x .
                    MonadIO m
                    => ParConduit m r1 i x
                    -> ParConduit m r2 x o
                    -> ParConduit m (r1, r2) i o
    fuseTuple = fuseMap (\r1 r2 -> (r1, r2))


    -- | Fuse two parallel conduits, appending the two results.
    --
    -- ![image](docs/fuseSemigroup.svg)
    --
    fuseSemigroup :: forall r m i o x .
                        (MonadIO m
                        , Semigroup r)
                        => ParConduit m r i x
                        -> ParConduit m r x o
                        -> ParConduit m r i o
    fuseSemigroup = fuseMap (<>)

