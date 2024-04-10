{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.LiftC
-- Description : Lifting normal conduits up into parallel conduits
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
module Data.Conduit.Parallel.Internal.LiftC (
    liftC,
    liftF,
    forceC
) where

    import           Control.DeepSeq
    import           Control.Exception                      (evaluate)
    import           Control.Monad.IO.Class
    import           Control.Monad.IO.Unlift                (MonadUnliftIO)
    import           Data.Conduit                           ((.|))
    import qualified Data.Conduit                           as C
    import           Data.Conduit.Parallel.Internal.Control
    import qualified Data.Conduit.Parallel.Internal.Duct    as Duct
    import           Data.Conduit.Parallel.Internal.Type    (ParConduit (..))
    import           Data.Void                              (Void)

    liftC :: forall m r i o .
                MonadUnliftIO m
                => C.ConduitT i o m r
                -> ParConduit m r i o
    liftC cdt = ParConduit go
        where
            go :: forall x .
                    Duct.ReadDuct i
                    -> Duct.WriteDuct o
                    -> Control x m (m r)
            go rd wd = spawnClient $ client rd wd

            client :: Duct.ReadDuct i
                    -> Duct.WriteDuct o
                    -> m r
            client src snk = do
                Duct.withReadDuct src Nothing $ \rd ->
                    Duct.withWriteDuct snk Nothing $ \wr ->
                        let c1 :: C.ConduitT () o m r
                            c1 = readConduit rd .| cdt

                            c2 :: C.ConduitT () Void m r
                            c2 = C.fuseUpstream c1 $ writeConduit wr
                        in
                        C.runConduit c2

            readConduit :: IO (Maybe i) -> C.ConduitT () i m ()
            readConduit rd = do
                x :: Maybe i <- liftIO rd
                case x of
                    Just v -> do
                        C.yield v
                        readConduit rd
                    Nothing -> pure ()

            writeConduit :: (o -> IO Duct.Open) -> C.ConduitT o Void m ()
            writeConduit wd = do
                x :: Maybe o <- C.await
                case x of
                    Nothing -> pure ()
                    Just v  -> do
                        open :: Duct.Open <- liftIO $ wd v
                        case open of
                            Duct.Open   -> writeConduit wd
                            Duct.Closed -> pure ()

    liftF :: forall a b m r .
                MonadUnliftIO m
                => (a -> m b)
                -> Duct.ReadDuct a
                -> Duct.WriteDuct b
                -> Control r m (m ())
    liftF f rda wdb = spawnClient go
        where
            go :: m ()
            go =
                Duct.withReadDuct rda Nothing $ \rd ->
                    Duct.withWriteDuct wdb Nothing $ \wr ->
                        let recur :: m ()
                            recur = do
                                ma <- liftIO $ rd
                                case ma of
                                    Nothing -> pure ()
                                    Just a  -> do
                                        b <- f a
                                        open :: Duct.Open <- liftIO $ wr b
                                        case open of
                                            Duct.Closed -> pure ()
                                            Duct.Open   -> recur
                        in
                        recur

    -- | Force the outputs of a ParConduit into normal form.
    forceC :: forall m r i o .
                NFData o
                => ParConduit m r i o
                -> ParConduit m r i o
    forceC pc = ParConduit go
        where
            go :: forall x . 
                    Duct.ReadDuct i
                    -> Duct.WriteDuct o
                    -> Control x m (m r)
            go rd wd =
                let wd' = Duct.contramapIO (evaluate . force) wd in
                getParConduit pc rd wd'


