{-# LANGUAGE InstanceSigs        #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Utils
-- Description : General Utils
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
module Data.Conduit.Parallel.Internal.Utils (
    readM,
    writeM,
    runM,
    Queue,
    makeQueue,
    withReadQueue,
    withWriteQueue
) where

    import           Control.Concurrent.STM                 (retry)
    import           Control.Monad.Cont
    import           Control.Monad.Trans.Maybe
    import qualified Data.Conduit.Parallel.Internal.ParDuct as Duct
    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Sequence                          (Seq)
    import qualified Data.Sequence                          as Seq
    import           Data.Void
    import           UnliftIO

    readM :: forall a m . MonadIO m => IO (Maybe a) -> MaybeT m a
    readM = MaybeT . liftIO
    {-# SPECIALIZE readM :: IO (Maybe a ) -> MaybeT IO a #-}

    writeM :: forall a m . MonadIO m => (a -> IO Duct.Open) -> a -> MaybeT m ()
    writeM wr a =
        MaybeT $ do
            open <- liftIO $ wr a
            case open of
                Duct.Open   -> pure $ Just ()
                Duct.Closed -> pure Nothing
    {-# SPECIALIZE writeM :: (a -> IO Duct.Open) -> a -> MaybeT IO () #-}

    runM :: forall m r . Monad m => MaybeT m Void -> Client r m ()
    runM act = do
        let act1 :: m (Maybe Void)
            act1 = runMaybeT act
        r :: Maybe Void <- lift $ act1
        case r of
            Nothing -> pure ()
            Just v  -> absurd v
    {-# SPECIALIZE runM :: MaybeT IO Void -> Worker () #-}


    type Queue a = TVar (Seq (Maybe a))

    makeQueue :: forall m a x .
                    MonadIO m
                    => Control x m (Queue a)
    makeQueue = liftIO $ newTVarIO (Seq.empty)

    withReadQueue :: forall m a r .
                        Queue a
                        -> Client r m (MaybeT IO a)
    withReadQueue que = pure $ doGet
        where
            doGet :: MaybeT IO a
            doGet = MaybeT . atomically $ do
                s <- readTVar que
                case Seq.viewl s of
                    Seq.EmptyL  -> retry
                    (Just a) Seq.:< s2 -> do
                        writeTVar que s2
                        pure $ Just a
                    Nothing Seq.:< _   -> do
                        pure Nothing

    withWriteQueue :: forall m a r .
                        MonadUnliftIO m
                        => Queue a
                        -> Client r m (a -> MaybeT IO ())
    withWriteQueue que = ContT go
        where
            go :: ((a -> MaybeT IO ()) -> m r) -> m r
            go f = finally (f writeQ) stop

            stop :: m ()
            stop = liftIO . atomically $ do
                        modifyTVar que (\s -> s Seq.|> Nothing)

            writeQ :: a -> MaybeT IO ()
            writeQ a = MaybeT . atomically $ do
                modifyTVar que (\s -> s Seq.|> (Just a))
                pure $ Just ()
