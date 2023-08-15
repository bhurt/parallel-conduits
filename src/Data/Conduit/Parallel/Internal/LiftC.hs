{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.LiftC
-- Description : Lifting normal conduits up into parallel conduits
-- Copyright   : (c) Brian Hurt, 2023
-- License     : BSD 3-clause
-- Maintainer  : bhurt42@gmail.com
-- Stability   : experimental
--
-- = Warning
--
-- This is an internal module of the Parallel Conduits.  You almost
-- certainly want to use [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- instead.  Anything in this module not explicitly re-exported 
-- by [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- is for internal use only, and will change or disappear without
-- notice.  Use at your own risk.
--
module Data.Conduit.Parallel.Internal.LiftC (
    liftC
) where

    import           Control.Monad.Cont                   (ContT)
    import           Control.Monad.IO.Class               (liftIO)
    import           Control.Monad.IO.Unlift              (MonadUnliftIO)
    import           Data.Conduit                         ((.|))
    import qualified Data.Conduit                         as C
    import qualified Data.Conduit.Parallel.Internal.Duct  as Duct
    import           Data.Conduit.Parallel.Internal.Spawn (spawn)
    import           Data.Conduit.Parallel.Internal.Type  (ParConduit (..))
    import           Data.Void                            (Void)
    import           UnliftIO.Exception                   (finally)

    liftC :: forall m r i o .
                MonadUnliftIO m
                => C.ConduitT i o m r
                -> ParConduit m r i o
    liftC cdt = ParConduit go
        where
            go :: forall x .
                    Duct.ReadDuct i
                    -> Duct.WriteDuct o
                    -> ContT x m (m r)
            go rd wd = spawn (work rd wd)

            work :: Duct.ReadDuct i
                    -> Duct.WriteDuct o
                    -> m r
            work rd wd = finally runc closeall
                where
                    runc :: m r
                    runc = 
                        let c1 :: C.ConduitT () o m r
                            c1 = readConduit rd .| cdt

                            c2 :: C.ConduitT () Void m r
                            c2 = C.fuseUpstream c1 $ writeConduit wd
                        in
                        C.runConduit c2

                    closeall :: m ()
                    closeall = liftIO $ do
                                            _ <- Duct.closeReadDuct rd
                                            Duct.closeWriteDuct wd

            readConduit :: Duct.ReadDuct i -> C.ConduitT () i m ()
            readConduit rd = do
                x <- liftIO $ Duct.readDuct rd
                case x of
                    Just v -> do
                        C.yield v
                        readConduit rd
                    Nothing -> pure ()

            writeConduit :: Duct.WriteDuct o -> C.ConduitT o Void m ()
            writeConduit wd = do
                x <- C.await
                case x of
                    Just v  -> do
                        open <- liftIO $ Duct.writeDuct wd v
                        case open of
                            Duct.Open   -> writeConduit wd
                            Duct.Closed -> pure ()
                    Nothing -> pure ()

