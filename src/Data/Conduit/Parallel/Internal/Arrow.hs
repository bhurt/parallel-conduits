{-# LANGUAGE InstanceSigs        #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Arrow
-- Description : ParArrow: parallel arrows.
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
module Data.Conduit.Parallel.Internal.Arrow (
    ParArrow(..),
    toParConduit
) where

    import qualified Control.Category                      as Cat
    import           Control.Monad.Cont                    (ContT)
    import           Data.Conduit.Parallel.Internal.Copier (copier)
    import qualified Data.Conduit.Parallel.Internal.Duct   as Duct
    import           Data.Conduit.Parallel.Internal.Spawn  (spawnIO)
    import           Data.Conduit.Parallel.Internal.Type
    import qualified Data.Functor.Contravariant            as Contra
    import qualified Data.Profunctor                       as Pro
    import           UnliftIO


    newtype ParArrow m i o = ParArrow {
                                    getParArrow ::
                                        forall x .
                                        Duct.ReadDuct i
                                        -> Duct.WriteDuct o
                                        -> ContT x m (m ())
                                }


    instance Functor (ParArrow m i) where
        fmap f c = ParArrow $
                    \r w -> getParArrow c r (Contra.contramap f w)

    instance Pro.Profunctor (ParArrow m) where
        dimap f g c = ParArrow $
                        \rd wd ->
                            getParArrow c
                                (fmap f rd)
                                (Contra.contramap g wd)
        lmap f c = ParArrow $
                    \rd wd ->
                        getParArrow c
                            (fmap f rd)
                            wd

        rmap g c = ParArrow $
                    \rd wd ->
                        getParArrow c
                            rd
                            (Contra.contramap g wd)

    instance (MonadUnliftIO m) => Cat.Category (ParArrow m) where
        id :: forall a . ParArrow m a a
        id = ParArrow go
                where
                    go :: Duct.ReadDuct a
                            -> Duct.WriteDuct a
                            -> ContT t m (m ())
                    go rd wr = spawnIO $ copier rd wr

        a1 . a2 = ParArrow $
                    \rd wd -> do
                        (rx, wx) <- liftIO $ Duct.newDuct
                        r1 <- getParArrow a2 rd wx
                        r2 <- getParArrow a1 rx wd
                        pure $ r1 >> r2

    -- | Convert a ParArrow to a ParConduit
    --
    -- As this is just unwrapping one newtype and wrapping a second
    -- of newtype, it should be free.
    --
    toParConduit :: forall m i o . ParArrow m i o -> ParConduit m () i o
    toParConduit a = ParConduit (getParArrow a)
        -- Note: as tempting as defining this function as:
        --      toParConduit = ParConduit . getParArrow
        -- This does not work, for reasons I am unclear about but have
        -- to do with weirdness around higher ranked types.

    {-
    liftK :: forall m i o .
                MonadUnliftIO m
                => Kleisli m i o
                -> ParArrow m i o
    liftK f = ParArrow $ \rd wd -> spawn $ go rd wd
        where
            go :: Duct.ReadDuct i
                    -> Duct.WriteDuct o
                    -> m ()
            go rd wd = 
    -}

