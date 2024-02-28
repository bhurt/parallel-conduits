{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.ParDuct
-- Description : Wrapper module to fit Ducts into the ParConduit library
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
-- = Purpose
--
-- This module wraps the various functions in `Duct` in such a way as
-- they work with the greater ParConduit ecosystem.  This lets Ducts
-- stay their own thing, and one day possibly be spun off into their
-- own library.
--
-- You should import this module rather than Duct itself.
--
module Data.Conduit.Parallel.Internal.ParDuct(
    Duct.Open(..),
    Duct.ReadDuct,
    Duct.WriteDuct,
    Duct.Duct,
    newDuct,
    newFullDuct,
    Duct.newClosedDuct,
    withReadDuct,
    withWriteDuct,
    addReadOpens,
    addWriteOpens,
    Duct.contramapIO
) where


    import           Control.Monad.Cont
    import qualified Data.Conduit.Parallel.Internal.Duct  as Duct
    import           Data.Conduit.Parallel.Internal.Spawn
    import           UnliftIO


    newDuct :: forall a m x . MonadIO m => Control x m (Duct.Duct a)
    newDuct = liftIO $ Duct.newDuct

    newFullDuct :: forall a m x . MonadIO m => a -> Control x m (Duct.Duct a)
    newFullDuct a = liftIO $ Duct.newFullDuct a

    withReadDuct :: forall a m r .
                    MonadUnliftIO m
                    => Duct.ReadDuct a
                    -> Client r m (IO (Maybe a))
    withReadDuct rd = ContT $ Duct.withReadDuct rd Nothing

    withWriteDuct :: forall a m r .
                        MonadUnliftIO m
                        => Duct.WriteDuct a
                        -> Client r m (a -> IO Duct.Open)
    withWriteDuct wd = ContT $ Duct.withWriteDuct wd Nothing

    addReadOpens :: forall a m x .
                    MonadIO m
                    => Duct.ReadDuct a
                    -> Int
                    -> Control x m ()
    addReadOpens rd n = liftIO $ Duct.addReadOpens rd n

    addWriteOpens :: forall a m x .
                        MonadIO m
                        => Duct.WriteDuct a
                        -> Int
                        -> Control x m ()
    addWriteOpens wd n = liftIO $ Duct.addWriteOpens wd n
