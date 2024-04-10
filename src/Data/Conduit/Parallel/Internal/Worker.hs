{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Worker
-- Description : Worker thread utilities.
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
-- = Purpose
--
-- This module provides various utility functions for writing Worker
-- threads. 
--
-- Like the "Data.Conduit.Parallel.Internal.Control" module, it includes
-- wrappers for the "Data.Conduit.Parallel.Internal.Duct" module,
-- specifically wrappers for the Duct functions called from worker
-- threads.
--
module Data.Conduit.Parallel.Internal.Worker(
    -- * Worker Loops
    --
    RecurM,
    runRecurM,

    -- * Ducts
    --
    Duct.ReadDuct,
    Duct.WriteDuct,
    Duct.Duct,
    withReadDuct,
    withWriteDuct,
    Reader,
    Writer,

    -- * Unbounded Queues
    --
    Queue,
    makeQueue,
    withReadQueue,
    withWriteQueue,

    -- * Bounded Queues
    BQueue,
    makeBQueue,
    withReadBQueue,
    withWriteBQueue

) where


    import           Control.Concurrent.STM
    import qualified Control.Exception                    as Ex
    import           Control.Monad.Cont
    import           Control.Monad.Trans.Maybe
    import qualified Data.Conduit.Parallel.Internal.Duct  as Duct
    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Sequence                        (Seq)
    import qualified Data.Sequence                        as Seq
    import           Data.Void

    -- | Worker loop type.
    --
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


    -- | Allow reading from a ReadDuct.
    --
    -- Converts a ReadDuct into a function that reads values from that
    -- read duct.  This function uses the standard Haskell with*
    -- pattern for wrapping `Control.Exception.bracket`.  When the
    -- wrapped computation exits (either via normal value return or
    -- due to an exception), the open count of the read duct is
    -- decremented.  If it drops to zero, then the read is closed.
    -- 
    withReadDuct :: forall a .  Duct.ReadDuct a -> Worker (Reader a)
    withReadDuct rd = do
        r :: IO (Maybe a) <- ContT $ Duct.withReadDuct rd Nothing
        pure $ MaybeT r

    -- | Allow writing to a WriteDuct.
    --
    -- Converts a WriteDuct into a function that writes values to that
    -- write duct.  This function uses the standard Haskell with*
    -- pattern for wrapping `Control.Exception.bracket`.  When the
    -- wrapped computation exits (either via normal value return or
    -- due to an exception), the open count of the write duct is
    -- decremented.  If it drops to zero, then the duct is closed.
    -- 
    withWriteDuct :: forall a . Duct.WriteDuct a -> Worker (Writer a)
    withWriteDuct wd = do
        wio :: (a -> IO Duct.Open) <- ContT $ Duct.withWriteDuct wd Nothing
        let wm :: a -> RecurM ()
            wm a = MaybeT $ do
                open <- wio a
                case open of
                    Duct.Open   -> pure $ Just ()
                    Duct.Closed -> pure Nothing
        pure wm

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

