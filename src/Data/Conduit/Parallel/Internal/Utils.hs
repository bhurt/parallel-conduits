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
-- This is an internal module of the Parallel Conduits library.  You
-- almost certainly want to use "Data.Conduit.Parallel"  instead.  
-- Anything in this module not explicitly re-exported by
-- "Data.Conduit.Parallel" is for internal use only, and will change
-- or disappear without notice.  Use at your own risk.
--
module Data.Conduit.Parallel.Internal.Utils (
    RecurM,
    runRecurM,
    Reader,
    Writer,
    Queue,
    makeQueue,
    withReadQueue,
    withWriteQueue,
    BQueue,
    makeBQueue,
    withReadBQueue,
    withWriteBQueue,
    finalize
) where

    import           Control.Concurrent.STM
    import qualified Control.Exception                    as Ex
    import           Control.Monad.Cont
    import           Control.Monad.Trans.Maybe
    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Sequence                        (Seq)
    import qualified Data.Sequence                        as Seq
    import           Data.Void

    type RecurM a = MaybeT IO a

    type Reader a = RecurM a

    type Writer a = a -> RecurM ()

    runRecurM :: RecurM Void -> Worker ()
    runRecurM act = lift $ do
        r :: Maybe Void <- runMaybeT act
        case r of
            Nothing -> pure ()
            Just v  -> absurd v

    finalize :: IO () -> Worker ()
    finalize fini = ContT go
        where
            go :: (() -> IO r) -> IO r
            go f = Ex.finally (f ()) fini

    data Cache a = Cache {
                        queue :: TVar (Seq a),
                        closed :: TVar Bool
                    }

    makeCache :: forall a m r . MonadIO m => Control r m (Cache a)
    makeCache = liftIO $ Cache <$> newTVarIO Seq.empty <*> newTVarIO False

    makeCacheReader :: forall a .  Cache a -> Reader a
    makeCacheReader cache = MaybeT . atomically $ do
        sa :: Seq a <- readTVar (queue cache)
        case Seq.viewl sa of
            a Seq.:< s2 -> do
                writeTVar (queue cache) s2
                pure $ Just a
            Seq.EmptyL -> do
                isClosed :: Bool <- readTVar (closed cache)
                if isClosed
                then pure Nothing
                else retry

    makeCacheWriter :: forall a .
                        (Seq a -> Bool)
                        -> Cache a
                        -> Writer a
    makeCacheWriter canWrite cache = \a -> MaybeT . atomically $ do
        isClosed <- readTVar (closed cache)
        if isClosed
        then pure Nothing
        else do
            s :: Seq a <- readTVar (queue cache)
            if (canWrite s)
            then do
                writeTVar (queue cache) (s Seq.:|> a)
                pure $ Just ()
            else
                retry


    closeCache :: forall a .  Cache a -> IO ()
    closeCache cache = atomically $ writeTVar (closed cache) True


    -- | Unbounded queues.
    --
    -- Used in Arrows, where the bounding is done by the ducts the
    -- Queues are bypassing.
    --
    newtype Queue a = Queue { getCache :: Cache a }

    makeQueue :: forall m a x .
                    MonadIO m
                    => Control x m (Queue a)
    makeQueue = Queue <$> makeCache

    withReadQueue :: forall a . Queue a -> Worker (Reader a)
    withReadQueue que = do
        finalize (closeCache (getCache que))
        pure $ makeCacheReader (getCache que)

    withWriteQueue :: forall a . Queue a -> Worker (Writer a)
    withWriteQueue que = do
        finalize (closeCache (getCache que))
        pure $ makeCacheWriter (const True) (getCache que)


    -- | Bounded sized queue
    --
    -- Used for caching in ParConduits and ParArrows.
    data BQueue a = BQueue { 
                        getBCache :: Cache a,
                        maxLen :: Int
                    }

    makeBQueue :: forall m a x .  MonadIO m => Int -> Control x m (BQueue a)
    makeBQueue size 
        | size <= 0 = error "Non-positive size for makeBQueue"
        | otherwise = do
            c <- makeCache
            pure $ BQueue { getBCache = c, maxLen = size }

    withReadBQueue :: forall a .  BQueue a -> Worker (Reader a)
    withReadBQueue bque = do
        finalize (closeCache (getBCache bque))
        pure $ makeCacheReader (getBCache bque)

    withWriteBQueue :: forall a .  BQueue a -> Worker (Writer a)
    withWriteBQueue bque = do
        finalize (closeCache (getBCache bque))
        pure $ makeCacheWriter
                (\s -> Seq.length s < maxLen bque)
                (getBCache bque)

