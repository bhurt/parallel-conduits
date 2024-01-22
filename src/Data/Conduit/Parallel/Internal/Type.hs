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
    fuseBase
) where

    import qualified Control.Category                     as Cat
    import           Control.Monad.Cont                   (ContT)
    import           Control.Monad.IO.Class
    import           Control.Monad.IO.Unlift              (MonadUnliftIO)
    import qualified Data.Conduit.Parallel.Internal.Duct  as Duct
    import           Data.Conduit.Parallel.Internal.Spawn (spawn)
    import qualified Data.Functor.Contravariant           as Contra
    import qualified Data.Profunctor                      as Pro


    newtype ParConduit m r i o = ParConduit {
                                    getParConduit :: 
                                        forall x .
                                        Duct.ReadDuct i
                                        -> Duct.WriteDuct o
                                        -> ContT x m (m r) }

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
    fuseBase :: forall r1 r2 r m i o x .
                    (MonadIO m)
                    => (Duct.WriteDuct x -> Duct.WriteDuct x)
                    -> (r1 -> r2 -> r)
                    -> ParConduit m r1 i x
                    -> ParConduit m r2 x o
                    -> ParConduit m r i o
    fuseBase modWrite fixr pc1 pc2 = ParConduit go
        where
            go :: forall t .
                    Duct.ReadDuct i
                    -> Duct.WriteDuct o
                    -> ContT t m (m r)
            go rd wd = do
                (xrd, xwd) <- liftIO $ Duct.newDuct
                let xwd' :: Duct.WriteDuct x
                    xwd' = modWrite xwd
                r1 <- getParConduit pc1 rd xwd'
                r2 <- getParConduit pc2 xrd wd
                pure $ fixr <$> r1 <*> r2


    instance (MonadUnliftIO m, Monoid r) => Cat.Category (ParConduit m r) where
        id :: forall a . ParConduit m r a a
        id = ParConduit go
                where
                    go :: Duct.ReadDuct a
                            -> Duct.WriteDuct a
                            -> ContT t m (m r)
                    go rd wr = spawn (work rd wr)

                    work :: Duct.ReadDuct a
                            -> Duct.WriteDuct a
                            -> m r
                    work rd wr = do
                        mx <- liftIO $ Duct.readDuct rd Nothing
                        case mx of
                            Nothing -> do
                                _ <- liftIO $ Duct.closeReadDuct rd
                                liftIO $ Duct.closeWriteDuct wr
                                pure mempty
                            Just x -> do
                                mr <- liftIO $ Duct.writeDuct wr Nothing x
                                case mr of
                                    Duct.Open -> work rd wr
                                    Duct.Closed -> do
                                        _ <- liftIO $ Duct.closeReadDuct rd
                                        liftIO $ Duct.closeWriteDuct wr
                                        pure mempty

        (.) = flip $ fuseBase Prelude.id mappend
