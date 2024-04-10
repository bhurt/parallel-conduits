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
    -- * Worker Thread Type
    --
    Worker,

    -- * Worker Loops
    --
    LoopM,
    runLoopM,

    -- * Ducts
    --
    Reader,
    Writer,
    Duct.ReadDuct,
    Duct.WriteDuct,
    Duct.Duct,
    openReadDuct,
    openWriteDuct,

    -- * Unbounded Queues
    --
    Queue,
    makeQueue,
    openReadQueue,
    openWriteQueue,

    -- * Bounded Queues
    BQueue,
    makeBQueue,
    openReadBQueue,
    openWriteBQueue,

    -- * Common Worker Threads
    copier,
    direct,

    -- * Convience
    Void
) where

    import           Control.Concurrent.STM
    import qualified Control.Exception                      as Ex
    import           Control.Monad.Cont
    import           Control.Monad.Trans.Maybe
    import           Data.Bitraversable
    import           Data.Conduit.Parallel.Internal.Control
    import qualified Data.Conduit.Parallel.Internal.Duct    as Duct
    import           Data.Sequence                          (Seq)
    import qualified Data.Sequence                          as Seq
    import           Data.Void




    -- | The worker thread type.
    --
    -- Worker threads are spawned by the control thread to do internal
    -- work- generally getting values from read ducts and writing them
    -- to write ducts.  As they do not execute client-supplied code (use
    -- client threads for that case), they do not need to execute in
    -- the client-supplied monad transformer stack.  They are thus
    -- somewhat more efficient to spawn.
    type Worker a = ContT () IO a

    -- | Worker loop type.
    --
    -- The general pattern of worker threads is that they get the read
    -- and write actions of some number of ducts or queues, then go into
    -- a loop read and writing, until something closes.  At which point
    -- they exit.
    --
    -- So worker threads start in the `Worker` monad, which is a 
    -- ContT monad to handle the opening of ducts and queues.  It then
    -- runs a MaybeT transformer handling all the pattern matching.
    --
    -- So a simple copy function might look like:
    --
    -- @
    --      copyWorker :: ReadDuct a -> WriteDuct a -> Worker ()
    --      copyWorker rd wd = do
    --          read <- openReadDuct rd
    --          write <- openWriteDuct wd
    --          let loop :: LoopM Void
    --              loop = do
    --                  a <- read
    --                  write a
    --                  loop
    --          in runLoopM loop
    -- @
    --
    -- Note that loops return void- they do not exit until something closes
    -- (returns Nothing).
    --
    type LoopM a = MaybeT IO a

    -- | Reader type.
    --
    -- Closures that produce (read) a value, either from a ReadDuct
    -- for from the read end of a queue.
    type Reader a = LoopM a

    -- | Writer type.
    --
    -- Closures that consume (write) a value, either to a WriteDuct
    -- or to the write end of a queue.
    type Writer a = a -> LoopM ()

    -- | Run a LoopM monad in a worker thread.
    runLoopM :: LoopM Void -> Worker ()
    runLoopM act = lift $ do
        r :: Maybe Void <- runMaybeT act
        case r of
            Nothing -> pure ()
            Just v  -> absurd v

    -- | Ensure some action is executed on completion in a worker thread.
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
    openReadDuct :: forall a .  Duct.ReadDuct a -> Worker (Reader a)
    openReadDuct rd = do
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
    openWriteDuct :: forall a . Duct.WriteDuct a -> Worker (Writer a)
    openWriteDuct wd = do
        wio :: (a -> IO Duct.Open) <- ContT $ Duct.withWriteDuct wd Nothing
        let wm :: a -> LoopM ()
            wm a = MaybeT $ do
                open <- wio a
                case open of
                    Duct.Open   -> pure $ Just ()
                    Duct.Closed -> pure Nothing
        pure wm

    -- | The shared struture of Queues.
    --
    -- As we have two different types of queues (bounded and unbounded),
    -- we want to factor out all the common code and types.
    --
    -- We'd like to call this a Queue, but, um, that name is already taken.
    data Cache a = Cache {
                        queue :: TVar (Seq a),
                        closed :: TVar Bool
                    }

    -- | The shared queue creation function.
    makeCache :: forall a m r . MonadIO m => Control r m (Cache a)
    makeCache = liftIO $ Cache <$> newTVarIO Seq.empty <*> newTVarIO False

    -- | The shared read end of a queue.
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

    -- | The shared write end of a queue.
    makeCacheWriter :: forall a .
                        (Seq a -> Bool)
                        -- ^ Returns false if we should block (retry).
                        --
                        -- This is the one difference between bounded and
                        -- unbounded queues.
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

    -- | Shared queue close code.
    closeCache :: forall a .  Cache a -> IO ()
    closeCache cache = atomically $ writeTVar (closed cache) True


    -- | Unbounded FIFO queues.
    --
    -- Used in Arrows, where the bounding is done by the ducts the
    -- Queues are bypassing.
    --
    newtype Queue a = Queue { getCache :: Cache a }

    -- | Make an unbounded FIFO Queue.
    makeQueue :: forall m a x .
                    MonadIO m
                    => Control x m (Queue a)
    makeQueue = Queue <$> makeCache

    -- | Open an unbounded FIFO Queue for reading.
    openReadQueue :: forall a . Queue a -> Worker (Reader a)
    openReadQueue que = do
        finalize (closeCache (getCache que))
        pure $ makeCacheReader (getCache que)

    -- | Open an unbounded FIFO Queue for writing.
    openWriteQueue :: forall a . Queue a -> Worker (Writer a)
    openWriteQueue que = do
        finalize (closeCache (getCache que))
        pure $ makeCacheWriter (const True) (getCache que)


    -- | Bounded sized queue
    --
    -- Used for caching in ParConduits and ParArrows.
    data BQueue a = BQueue { 
                        getBCache :: Cache a,
                        maxLen :: Int
                    }

    -- | Create a bounded FIFO Queue.
    makeBQueue :: forall m a x .  MonadIO m => Int -> Control x m (BQueue a)
    makeBQueue size 
        | size <= 0 = error "Non-positive size for makeBQueue"
        | otherwise = do
            c <- makeCache
            pure $ BQueue { getBCache = c, maxLen = size }

    -- | Open a bounded FIFO Queue for reading.
    openReadBQueue :: forall a .  BQueue a -> Worker (Reader a)
    openReadBQueue bque = do
        finalize (closeCache (getBCache bque))
        pure $ makeCacheReader (getBCache bque)

    -- | Open a bounded FIFO Queue for writing.
    openWriteBQueue :: forall a .  BQueue a -> Worker (Writer a)
    openWriteBQueue bque = do
        finalize (closeCache (getBCache bque))
        pure $ makeCacheWriter
                (\s -> Seq.length s < maxLen bque)
                (getBCache bque)

    -- | The common copier worker thread.
    --
    -- This worker thread just copies values from a single ReadDuct
    -- into a single WriteDuct.
    --
    copier :: forall a .
                ReadDuct a
                -> WriteDuct a
                -> Worker ()
    copier src snk = do
        reada :: Reader a <- openReadDuct src
        writea :: Writer a <- openWriteDuct snk
        let recur :: LoopM Void
            recur = do
                a <- reada
                writea a
                recur
        runLoopM recur

    -- | Common Worker thread to split bitraversable values.
    --
    -- This worker thread reads bitraversable values from a read duct,
    -- and traverse over them writing their parts to the appropriate
    -- write ducts.
    --
    -- Note that we use the Bitraversable type class here to support
    -- both Either and tuple (,) values, as well as types not directly
    -- referenced by this library, like
    -- [These](https://hackage.haskell.org/package/these-1.2/docs/Data-These.html).
    --
    direct :: forall a b f .
                Bitraversable f
                => ReadDuct (f a b)
                -> WriteDuct a
                -> WriteDuct b
                -> Worker ()
    direct rdf wda wdb = do
        readf  :: Reader (f a b) <- openReadDuct rdf
        writea :: Writer a       <- openWriteDuct wda
        writeb :: Writer b       <- openWriteDuct wdb
        let recur :: LoopM Void
            recur = do
                f :: f a b <- readf
                _ <- bitraverse writea writeb f
                recur
        runLoopM recur



