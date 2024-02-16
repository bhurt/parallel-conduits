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
    toParConduit,
    liftK
) where

    import           Control.Arrow
    import qualified Control.Category                      as Cat
    import           Control.Monad.Cont                    (ContT)
    import           Data.Conduit.Parallel.Internal.AUtils
    import           Data.Conduit.Parallel.Internal.Copier (copier)
    import qualified Data.Conduit.Parallel.Internal.Duct   as Duct
    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Conduit.Parallel.Internal.Type
    import qualified Data.Functor.Contravariant            as Contra
    import qualified Data.Profunctor                       as Pro
    import           Data.These
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

    instance MonadUnliftIO m => Arrow (ParArrow m) where
        arr f = ParArrow $ runK (pure . f)

        first :: forall b c d .
                    ParArrow m b c
                    -> ParArrow m (b, d) (c, d)
        first pa = ParArrow $ aBypass f g (getParArrow pa)
            where

                f :: (b, d) -> These d b
                f (b, d) = These d b

                g :: These d c -> (c, d)
                g (These d c) = (c, d)
                g _           = error "Impossible state reached!"

        second :: forall b c d .
                    ParArrow m b c
                    -> ParArrow m (d, b) (d, c)
        second pa = ParArrow $ aBypass f g (getParArrow pa)
            where
                f :: (d, b) -> These d b
                f (d, b) = These d b

                g :: These d c -> (d, c)
                g (These d c) = (d, c)
                g _           = error "Impossible state reached!"

        (***) :: forall b c b' c' .
                    ParArrow m b c
                    -> ParArrow m b' c'
                    -> ParArrow m (b, b') (c, c')
        p1 *** p2 = ParArrow $ aDupe f g (getParArrow p1) (getParArrow p2)
            where 
                f :: (b, b') -> These b b'
                f (b, b') = These b b'
                g :: These c c' -> (c, c')
                g (These c c') = (c, c')
                g _ = error "Impossible state reached!"

        (&&&) :: forall b c c' .
                    ParArrow m b c
                    -> ParArrow m b c'
                    -> ParArrow m b (c, c')
        p1 &&& p2 = ParArrow $ aDupe f g (getParArrow p1) (getParArrow p2)
            where
                f :: b -> These b b
                f b = These b b
                g :: These c c' -> (c, c')
                g (These c c') = (c, c')
                g _ = error "Impossible state reached!"

    instance MonadUnliftIO m => ArrowChoice (ParArrow m) where

        left :: forall b c d .
                    ParArrow m b c
                    -> ParArrow m (Either b d) (Either c d)
        left pa = ParArrow $ aBypass f g (getParArrow pa)
            where
                f :: Either b d -> These d b
                f (Left b)  = That b
                f (Right d) = This d

                g :: These d c -> Either c d
                g (This d) = Right d
                g (That c) = Left c
                g _        = error "Impossible state reached!"

        right :: forall b c d .
                    ParArrow m b c
                    -> ParArrow m (Either d b) (Either d c)
        right pa = ParArrow $ aBypass f g (getParArrow pa)
            where

                f :: Either d b -> These d b
                f (Left d)  = This d
                f (Right b) = That b

                g :: These d c -> Either d c
                g (This d) = Left d
                g (That c) = Right c
                g _        = error "Impossible state reached!"

        (+++) :: forall b c b' c' .
                    ParArrow m b c
                    -> ParArrow m b' c'
                    -> ParArrow m (Either b b') (Either c c')
        pa1 +++ pa2 = ParArrow $ aDupe f g (getParArrow pa1) (getParArrow pa2)
            where
                f :: Either b b' -> These b b'
                f (Left b)   = This b
                f (Right b') = That b'

                g :: These c c' -> Either c c'
                g (This c) = Left c
                g (That c') = Right c'
                g _ = error "Impossible state reached!"


        (|||) :: forall b c d .
                    ParArrow m b d
                    -> ParArrow m c d
                    -> ParArrow m (Either b c) d
        pa1 ||| pa2 = ParArrow $ aDupe f g (getParArrow pa1) (getParArrow pa2)
            where
                f :: Either b c -> These b c
                f (Left b)  = This b
                f (Right c) = That c

                g :: These d d -> d
                g (This d) = d
                g (That d) = d
                g _ = error "Impossible state reached!"

    instance MonadUnliftIO m => ArrowLoop (ParArrow m) where
        loop :: forall b c d .
                    ParArrow m (b, d) (c, d)
                    -> ParArrow m b c
        loop inner = ParArrow $ aLoop (getParArrow inner)

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

    -- | Lift a Kleisli arrow into a ParArrow.
    --
    -- This creates a ParArrow that spawns a single thread, which calls
    -- the Kleisli function on every value.  Note that since `Kleisli`
    -- is itself an arrow, it can represent an arbitrary complicated
    -- pipeline itself.  But this entire pipeline will be executed in
    -- a single thread.
    liftK :: forall m i o .
                MonadUnliftIO m
                => Kleisli m i o
                -> ParArrow m i o
    liftK kl = ParArrow $ runK (runKleisli kl)

