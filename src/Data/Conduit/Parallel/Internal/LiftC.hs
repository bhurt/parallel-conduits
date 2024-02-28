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
-- This is an internal module of the Parallel Conduits library.  You almost
-- certainly want to use [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- instead.  Anything in this module not explicitly re-exported 
-- by [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- is for internal use only, and will change or disappear without
-- notice.  Use at your own risk.
--
module Data.Conduit.Parallel.Internal.LiftC (
    liftC,
    forceC
) where

    import           Control.DeepSeq
    import           Control.Exception                      (evaluate)
    import           Control.Monad.IO.Class                 (liftIO)
    import           Control.Monad.IO.Unlift                (MonadUnliftIO)
    import           Control.Monad.Trans                    (lift)
    import           Data.Conduit                           ((.|))
    import qualified Data.Conduit                           as C
    import           Data.Conduit.Parallel.Internal.ParDuct
    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Conduit.Parallel.Internal.Type    (ParConduit (..))
    import           Data.Void                              (Void)

    liftC :: forall m r i o .
                MonadUnliftIO m
                => C.ConduitT i o m r
                -> ParConduit m r i o
    liftC cdt = ParConduit go
        where
            go :: forall x .
                    ReadDuct i
                    -> WriteDuct o
                    -> Control x m (m r)
            go rd wd = spawnClient $ client rd wd

            client :: ReadDuct i
                    -> WriteDuct o
                    -> Client r m r
            client src snk = do
                rd <- withReadDuct src
                wd <- withWriteDuct snk
                let c1 :: C.ConduitT () o m r
                    c1 = readConduit rd .| cdt

                    c2 :: C.ConduitT () Void m r
                    c2 = C.fuseUpstream c1 $ writeConduit wd
                lift $ C.runConduit c2

            readConduit :: IO (Maybe i) -> C.ConduitT () i m ()
            readConduit rd = do
                x <- liftIO rd
                case x of
                    Just v -> do
                        C.yield v
                        readConduit rd
                    Nothing -> pure ()

            writeConduit :: (o -> IO Open) -> C.ConduitT o Void m ()
            writeConduit wd = do
                x <- C.await
                case x of
                    Just v  -> do
                        open <- liftIO $ wd v
                        case open of
                            Open   -> writeConduit wd
                            Closed -> pure ()
                    Nothing -> pure ()


    -- | Force the outputs of a ParConduit into normal form.
    forceC :: forall m r i o .
                NFData o
                => ParConduit m r i o
                -> ParConduit m r i o
    forceC pc = ParConduit go
        where
            go :: forall x .  ReadDuct i -> WriteDuct o -> Control x m (m r)
            go rd wd =
                let wd' = contramapIO (evaluate . force) wd in
                getParConduit pc rd wd'


