{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Duct
-- Description : Ducts (aka Closable MVars)
-- Copyright   : (c) Brian Hurt, 2023
-- License     : BSD 3-clause
-- Maintainer  : bhurt42@gmail.com
-- Stability   : experimental
--
-- Try 1:
--
-- ![image Try 1](docs/fuse.svg)
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
-- = Purpose
--
-- This module implements Ducts (aka Closable MVars).  A duct is a one
-- element channel between two threads.  It works much like an MVar does,
-- in the reading the duct when it's empty blocks, as does writting to the
-- duct when it's full. 
--
-- But in addition to being full or empty, ducts can also be closed.  A
-- closed duct does not allow any further values to be transmitted, and
-- both reads and writes fail.  In this sense, ducts act like unix pipes,
-- except that values do not need to be serialized and deserialized,
-- and communication only happens within a single process.
--
-- Reads and writes to ducts are guaranteed to be in FIFO order.  In
-- addition ducts guarantee single wake-up, like MVars do.
--
-- As ducts act only as channels of communication, and not as synchronized
-- mutable variables the way MVars do, we deviate from the MVar API.
-- Instead of take and put, you read and write.  And instead of having
-- a single Duct type, we have two endpoints, a ReadDuct endpoint (that
-- can be read from) and a WriteDuct endpoint.
--
module Data.Conduit.Parallel.Internal.Duct(
    Open(..),
    ReadDuct,
    WriteDuct,
    newDuct,
    newFullDuct,
    newClosedDuct,
    closeReadDuct,
    closeWriteDuct,
    readDuct,
    writeDuct,
    contramapIO
) where

    import           Control.Concurrent.STM
    import           Control.Exception          (assert, bracket)
    import           Control.Monad.State
    import           Data.Functor.Contravariant
    import           Data.Sequence              (Seq)
    import qualified Data.Sequence              as Seq
    import           GHC.Generics               (Generic)

    -- **********************************************************************
    -- **                                                                  **
    -- **                      Developer's Commentary                      **
    -- **                                                                  **
    -- **********************************************************************
    --
    -- Hoo boy, has this module seen a lot of rewrites to get right.  I
    -- wanted to summarize the development history here, to forestall
    -- objections of "why didn't you do this in a simpler way?"  
    --
    -- *** Previous Implementations
    --
    -- - The obvious MVar (Maybe a), where Nothing means the Duct is
    --   closed, doesn't allow the readers to signal to the writers
    --   that they have exited.
    --
    -- - The obvious TVar (Maybe a) implementation fails due to the
    --   thundering herd problem- every change of the state wakes up
    --   every thread blocking on the Duct, even if all but one of
    --   them immediately blocks again.
    --
    -- - Trying to add an additional bit of mutable state (either a TVar
    --   or an atomic boolean) to an MVar has problems with race conditions,
    --   and with keeping track of how many threads are blocked on the MVar
    --   (and what operations they are blocked on), to allow us to clear
    --   out the blocking threads on a close.
    --
    -- - Using two MVars, one a Maybe a (where Nothing means the Duct is
    --   closed) and the other Bool (False meaning closed), where you
    --   do a take on one MVar and after that succeeds does a tryPut
    --   on the other, has problems where an ill-timed async exception
    --   (between the take and the tryPut) can leave the Duct in an
    --   inconsistent state.
    --
    -- - A variation where extra threads where spawned to ensure that
    --   only one thread was ever waiting on any given TVar was tried.
    --   This gave rise to an unacceptable number of threads needed to
    --   just copy values from one TVar into another.
    --
    -- - Several variations of the current solution were tried, that
    --   turned out to have either race conditions, thundering herd
    --   problems, or problems with async exceptions leaving the Duct
    --   in an inconsistant state.  This is dicussed below.
    --
    -- *** Current Implementation
    --
    -- The core idea of the current implementation is to hand manage the
    -- blocking.  Instead of blocking (retrying) on the TVar holding the
    -- main status, every thread has it's own unique TVar (called a
    -- Waiter) that only it blocks on.  When a thread blocks on the Duct,
    -- it adds it's waiter to the right queue (one queue for readers, one
    -- for writers) and then commits the transaction.  Then, in a new
    -- transaction, it blocks on it's waiter.  When a thread completes
    -- an action (a take or put), it takes the first waiter off the
    -- other queue and wakes it up by writing to the TVar.  This wakes
    -- up precisely one thread and solves the thundering herd problem.
    --
    -- So now our algorithm looks like:
    --      - Start the first transaction
    --      - If we don't need to block (i.e. no one is already in our
    --          operation's queue, and we can perform our operation
    --          immediately):
    --          - perform the operation
    --          - commit the first transaction
    --          - return
    --      - else:
    --          - add our waiter to our operation's queue
    --          - commit the first transaction
    --          - start the second transaction
    --          - block on our waiter.  Note that since this is first TVar
    --              we access in the current transaction, it is the only
    --              TVar we wake up for.
    --          - remove ourselves from the right queue
    --          - perform operation
    --          - commit the second transaction
    --          - return
    --
    -- But now we have another problem: we're doing two transactions
    -- instead of just one.  Now async exceptions can happen.  Specifically
    -- one can happen after the first transaction commits and before the
    -- second one commits.  So now we need some sort of try/catch to catch
    -- the async exception and perform the needed cleanup.
    --
    -- Trying to only do the try/catch around the second transaction lead
    -- to an inevitable race condition- there always seemed to be the
    -- possibility of an async exception after the first transaction
    -- committed, but before a try/catch could take effect.  Even if it
    -- was only a few clock cycles.  It caused bad flashbacks to the days
    -- I did device drivers and had to worry about hardware interrupts and
    -- critical sections, and also a lot of reading and research into
    -- Exception.mask and friends.
    --
    -- The solution is to put the try/catch around both transactions.  Then
    -- we need to allocate the waiter, whether we need to block or not.  The
    -- waiter tracks what cleanup needs to be done.
    --
    -- The cleanup that needs to be done is that when we've blocked, we
    -- need to remove ourselves from the queue, and then maybe wake up
    -- the next waiter in the queue (if we ourselves were woken up).
    -- Rather than searching the whole queue to remove ourselves (an
    -- O(n) operation), we just mark our waiter as having been abandoned-
    -- i.e. no thread is waiting on it.  When we awaken the first thread
    -- in the queue, we may need to remove several abandoned waiters first.
    --
    -- An earlier version had waiters for reads accept the value to return.
    -- The advantage of doing is that they then didn't need to reread
    -- the main status tvar, and could just directly return.  But this
    -- caused problems in clean up.  For example, consider a read that
    -- throws an async exception.  What to do with the value you've
    -- been handed?  What you want to do is to put it back in the mvar
    -- and awaken the next reader to get the value.  But some other writer
    -- has snuck in and fill the Duct before you can do that- indeed,
    -- some arbitrary number of reads and writes may have happened in the
    -- meantime, so that even if you do put the value back, it's now in
    -- wrong order (values put after the given value have been taken
    -- before).  The solution is to have a thread remove itself from
    -- the queue, and remove or add the value to the Duct.

    -- | Boolean-analog for whether the Duct is open or closed.
    --
    -- Note that Ducts can be closed, which means that writeDuct can
    -- fail if the Duct is closed.  So instead of returning ()
    -- like putMVar does, `writeDuct` returns a Open value.  A value
    -- of Open means the operation succeeded, a value of Closed means
    -- it failed.
    data Open =
        Open        -- ^ The Duct is Open
        | Closed    -- ^ The Duct is Closed
        deriving (Show, Read, Ord, Eq, Enum, Bounded, Generic)

    -- | The status of a waiter.
    data WaiterStatus =
        Waiting         -- ^ A thread is blocked waiting on this waiter
        | Awoken        -- ^ A thread blocked on this waiter has been awoken,
                        -- but has not yet woken up.
        | Abandoned     -- ^ No thread is blocked on this waiter, either
                        -- because it hasn't blocked yet, has awoken and
                        -- completed it's action, or because an async
                        -- exception has caused the waiter to be abandoned.
        | Closing       -- ^ The Duct has been closed.

    -- | A thread blocking waiting for it's turn.
    newtype Waiter = Waiter {
        waiterTVar :: TVar WaiterStatus
    } deriving (Eq)

    {-
    For debugging, we may want:
    data Waiter = Waiter {
        waiterTVar :: TVar WaiterStatus,
        waiterThreadId :: ThreadId m
    } deriving (Eq)
    -}

    -- Just like the idea behind creating boolean-analogs to prevent
    -- boolean-blindness, we create Maybe-analogs.  I started getting
    -- lots of different Maybes flying around, meaning different things.
    -- And this lead to bugs where I mixed up which maybes meant what.
    -- So I replaced all Maybes with explicit analogs.  The one Maybe
    -- that remains is the one the user sees.

    -- | Maybe-analog as to whether the duct contains a value or not.
    data Contents a =
        Full a
        | Empty

    -- | The current state of a Duct.
    --
    -- This used to be called the State, but then I wanted to use a StateT
    -- transformer, and the name clash caused sadness in the compiler.  So
    -- it got renamed.
    data Status a = Status {
                        sreaders :: Seq Waiter,
                        swriters :: Seq Waiter,
                        sclosing :: Bool,
                        scontents :: Contents a }

    -- | The read endpoint of a duct.
    data ReadDuct a = ReadDuct { 
                            readDuct :: IO (Maybe a),
                            closeReadDuct :: IO (Maybe a) }

    instance Functor ReadDuct where
        fmap f rd = ReadDuct {
                        readDuct = fmap f <$> readDuct rd,
                        closeReadDuct = fmap f <$> closeReadDuct rd }

    -- | The write endpoint of a duct.
    data WriteDuct a = WriteDuct {
                            writeDuct :: a -> IO Open,
                            closeWriteDuct :: IO () }

    instance Contravariant WriteDuct where
        contramap f wd = wd { writeDuct = writeDuct wd . f }

    -- | Maybe-analog for whether an operation has completed or would block.
    data WouldBlock a =
        Complete a
        | WouldBlock

    -- | Maybe-analog for whether the duct is closed or not.
    data IsClosed a =
        IsOpen a
        | IsClosed

    type StatusTVar a = TVar (IsClosed (Status a))

    type StatusM a = StateT (IsClosed (Status a)) STM

    isClosed :: forall a . StatusM a Bool
    isClosed = do
        s <- get
        pure $ case s of
                IsClosed -> True
                IsOpen _ -> False

    data SLens a x = SLens {
        view :: Status a -> x,
        update :: x -> Status a -> Status a
    }

    readers :: forall a . SLens a (Seq Waiter)
    readers = SLens {
                view = sreaders,
                update = \x s -> s { sreaders = x } }

    writers :: forall a . SLens a (Seq Waiter)
    writers = SLens {
                view = swriters,
                update = \x s -> s { swriters = x } }

    contents :: forall a . SLens a (Contents a)
    contents = SLens {
                    view = scontents,
                    update = \x s -> s { scontents = x } }

    closing :: forall a . SLens a Bool
    closing = SLens {
                    view = sclosing,
                    update = \x s -> s { sclosing = x } }

    viewM :: SLens a x -> StatusM a x
    viewM lens = do
        r <- get
        case r of
            IsClosed -> error "viewM on closed duct!"
            IsOpen s -> pure $ view lens s

    give :: SLens a x -> x -> StatusM a ()
    give lens val =
        modify (\case
                    IsClosed -> IsClosed
                    IsOpen s -> IsOpen $ update lens val s)

    mutate :: SLens a x -> (x -> x) -> StatusM a ()
    mutate lens f =
        modify (\case
                    IsClosed -> IsClosed
                    IsOpen s -> IsOpen $ update lens (f (view lens s)) s)

    closeStatusM :: StatusM a ()
    closeStatusM = put IsClosed

    addWaiter :: forall a .
                    SLens a (Seq Waiter)
                    -> Waiter
                    -> StatusM a ()
    addWaiter lens w = mutate lens (Seq.|> w)

    getQueueHead :: forall a .
                        SLens a (Seq Waiter)
                        -> StatusM a (Maybe Waiter)
    getQueueHead queue = do
            q :: Seq Waiter <- viewM queue
            loop False q
        where
            loop :: Bool -> Seq Waiter -> StatusM a (Maybe Waiter)
            loop needSave q = do
                case Seq.viewl q of
                    Seq.EmptyL  -> do
                        saveQueue needSave q
                        pure Nothing
                    w Seq.:< ws -> do
                        s :: WaiterStatus <- lift . readTVar $ waiterTVar w
                        case s of
                            Abandoned -> loop True ws
                            _         -> do
                                saveQueue needSave q
                                pure $ Just w

            saveQueue :: Bool -> Seq Waiter -> StatusM a ()
            saveQueue False _ = pure ()
            saveQueue True  q = give queue q


    awaken :: forall a .  SLens a (Seq Waiter) -> StatusM a ()
    awaken q = do
        r <- getQueueHead q
        case r of
            Nothing -> pure ()
            Just w  -> lift $ writeTVar (waiterTVar w) Awoken

    removeHead :: forall a .
                    SLens a (Seq Waiter)
                    -> Waiter
                    -> StatusM a ()
    removeHead queue me = do
        q :: Seq Waiter <- viewM queue
        case Seq.viewl q of
            Seq.EmptyL  -> error "removeHead called on empty queue!"
            h Seq.:< ws ->
                assert (me == h) $ do
                    give queue ws
                    pure ()

    ifM :: forall m a . Monad m => m Bool -> m a -> m a -> m a
    ifM testM thenM elseM = do
        t <- testM
        if t
        then thenM
        else elseM

    withWaiter :: forall a b .
                    StatusTVar b
                    -> (Waiter -> IO a)
                    -> IO a
    withWaiter tvar = bracket makeWaiter doCleanUp
        where
            makeWaiter :: IO Waiter
            makeWaiter = Waiter <$> newTVarIO Abandoned
                            -- <*> myThreadId

            doCleanUp :: Waiter -> IO ()
            doCleanUp waiter =
                atomically $ do
                    writeTVar (waiterTVar waiter) Abandoned
                    runStatusM tvar $ do
                        ifM isClosed (pure ()) $ do
                            cnts <- viewM contents
                            case cnts of
                                Full _ -> awaken readers
                                Empty  -> ifM (viewM closing)
                                            (pure ()) 
                                            (awaken writers)

    runStatusM :: forall a b .
                    StatusTVar a
                    -> StatusM a b
                    -> STM b
    runStatusM tvar go = do
        s1 :: IsClosed (Status a) <- readTVar tvar
        (b, s2) :: (b, IsClosed (Status a)) <- runStateT go s1
        writeTVar tvar s2
        pure b

    block :: forall a x .
                SLens a (Seq Waiter)
                -> Waiter
                -> StatusM a (WouldBlock x)
    block queue waiter = do
        lift $ writeTVar (waiterTVar waiter) Waiting
        addWaiter queue waiter
        pure WouldBlock

    getInLine :: forall a x .
                    SLens a (Seq Waiter)
                    -> Waiter
                    -> StatusM a (WouldBlock x)
                    -> StatusM a (WouldBlock x)
    getInLine queue waiter act = do
        -- Are there other waiters ahead of us in the queue?
        r <- getQueueHead queue
        case r of
            Just _ -> do
                -- Yes, there are other waiters ahead of us in
                -- the queue.  Add ourselves to the queue.
                block queue waiter
            Nothing -> do
                -- Nope, no other waiters ahead of us.
                act

    waitForWaiter :: forall x .  Waiter -> x -> STM x -> STM x
    waitForWaiter waiter onClosed act = do
        ws :: WaiterStatus <- readTVar (waiterTVar waiter)
        case ws of
            Waiting   -> retry          -- Block
            Abandoned -> pure onClosed
            Closing   -> pure onClosed
            Awoken    -> act

    handleBlock :: forall x .
                    (Waiter -> STM (WouldBlock x))
                    -> (Waiter -> STM x)
                    -> Waiter
                    -> IO x
    handleBlock preblock postblock waiter = do
        r <- atomically $ preblock waiter
        case r of
            Complete x -> pure x
            WouldBlock -> atomically $ postblock waiter

    setContents :: forall a .
                    Contents a
                    -> StatusM a ()
    setContents cnts = do
        give contents cnts
        case cnts of
            Empty -> awaken writers
            Full _ -> awaken readers

    closeAll :: Seq Waiter -> STM ()
    closeAll = mapM_ go 
        where
            go :: Waiter -> STM ()
            go w = writeTVar (waiterTVar w) Closing

    doReadDuct :: forall a .
                    StatusTVar a
                    -> IO (Maybe a)
    doReadDuct tvar = withWaiter tvar $
                                handleBlock preblock postblock
        where
            preblock :: Waiter -> STM (WouldBlock (Maybe a))
            preblock waiter = runStatusM tvar $ do
                -- Are we even open?  If not, return Nothing.
                ifM isClosed (pure (Complete Nothing)) $ do
                    getInLine readers waiter $ do
                        -- Look the contents of the duct.
                        cnts :: Contents a <- viewM contents
                        case cnts of
                            Empty  -> do
                                -- The duct is empty.  Block until it
                                -- isn't.
                                block readers waiter
                            Full a -> do
                                emptyContents (Complete (Just a))

            postblock :: Waiter -> STM (Maybe a)
            postblock waiter = 
                -- wait to be woken up.
                waitForWaiter waiter Nothing $ do
                    runStatusM tvar $ do
                        -- Make sure we are still open
                        ifM isClosed (pure Nothing) $ do
                            -- remove us from the queue
                            removeHead readers waiter
                            -- Get the contents
                            cnts :: Contents a <- viewM contents
                            case cnts of
                                Empty  ->
                                    -- We should not have been woken up
                                    -- if the duct is empty.
                                    error $ "readDuct woken up on "
                                            ++ "empty duct."
                                Full a -> do
                                    -- The duct is not empty.  Empty it.
                                    emptyContents (Just a)

            emptyContents :: forall r . r -> StatusM a r
            emptyContents r = do
                                -- The duct is not empty.  If we are closing,
                                -- close the duct.  Otherwise, empty the
                                -- contents, then return the value.
                                ifM (viewM closing)
                                    closeStatusM
                                    $ setContents Empty
                                pure r

    doCloseReadDuct :: forall a .
                        StatusTVar a
                        -> IO (Maybe a)
    doCloseReadDuct tvar = atomically $ do
        ms :: IsClosed (Status a) <- readTVar tvar
        case ms of
            IsClosed -> pure Nothing
            IsOpen s -> do
                writeTVar tvar IsClosed
                closeAll (view readers s)
                closeAll (view writers s)
                pure $ case view contents s of
                            Empty -> Nothing
                            Full a -> Just a

    doWriteDuct :: forall a .
                    StatusTVar a
                    -> a
                    -> IO Open
    doWriteDuct tvar val = withWaiter tvar $
                                    handleBlock preblock postblock
        where
            preblock :: Waiter -> STM (WouldBlock Open)
            preblock waiter = runStatusM tvar $ do
                -- Are we even open?  If not, return Closed.
                ifM isClosed (pure (Complete Closed)) $ do
                    -- Note: we can be closed for writing, even if we're
                    -- still open for reading.
                    ifM (viewM closing) (pure (Complete Closed)) $ do
                        getInLine writers waiter $ do
                            -- Look the contents of the duct.
                            cnts :: Contents a <- viewM contents
                            case cnts of
                                Empty  -> do
                                    -- The duct is empty.  Fill it.
                                    setContents (Full val)
                                    pure (Complete Open)
                                Full _ -> do
                                    -- The duct is not empty.  Block until
                                    -- it is.
                                    block writers waiter


            postblock :: Waiter -> STM Open
            postblock waiter = 
                waitForWaiter waiter Closed $ do
                    runStatusM tvar $ do
                        -- Make sure we are still open
                        ifM isClosed (pure Closed) $ do
                            ifM (viewM closing) (pure Closed) $ do
                                removeHead writers waiter
                                -- Get the contents
                                cnts :: Contents a <- viewM contents
                                case cnts of
                                    Empty  -> do
                                        setContents (Full val)
                                        pure Open
                                    Full _ -> do
                                        -- We should not have been woken up
                                        -- if the duct is full.
                                        error $ "writeDuct woken up on "
                                                ++ "full duct."


    doCloseWriteDuct :: forall a .  StatusTVar a -> IO ()
    doCloseWriteDuct tvar = atomically $ do
        ms :: IsClosed (Status a) <- readTVar tvar
        case ms of
            IsClosed -> pure ()
            IsOpen s -> do
                case view contents s of
                    Empty -> do
                        writeTVar tvar IsClosed
                        closeAll (view readers s)
                        closeAll (view writers s)
                    Full _ -> do
                        let s2 = update writers Seq.empty s
                            s3 = update closing True s2
                        writeTVar tvar (IsOpen s3)
                        closeAll (view writers s)

    makeDuct :: forall a . StatusTVar a -> (ReadDuct a, WriteDuct a)
    makeDuct tvar = (rd, wd)
        where
            rd :: ReadDuct a
            rd = ReadDuct {
                    readDuct = doReadDuct tvar,
                    closeReadDuct = doCloseReadDuct tvar }
            wd :: WriteDuct a
            wd = WriteDuct {
                    writeDuct = doWriteDuct tvar,
                    closeWriteDuct = doCloseWriteDuct tvar }

    makeStatus :: forall a . Contents a -> IsClosed (Status a)
    makeStatus r = IsOpen $
                        Status {
                            sreaders = mempty,
                            swriters = mempty,
                            sclosing = False,
                            scontents = r }

    newDuct :: forall a .  IO (ReadDuct a, WriteDuct a)
    newDuct = makeDuct <$> newTVarIO (makeStatus Empty) 

    newFullDuct :: forall a .
                    a -> IO (ReadDuct a, WriteDuct a)
    newFullDuct a = makeDuct <$> newTVarIO (makeStatus (Full a))

    newClosedDuct :: forall a . IO (ReadDuct a, WriteDuct a)
    newClosedDuct = pure (rd, wd)
        where
            rd :: ReadDuct a
            rd = ReadDuct {
                    readDuct = pure Nothing,
                    closeReadDuct = pure Nothing }
            wd :: WriteDuct a
            wd = WriteDuct {
                    writeDuct = const (pure Closed),
                    closeWriteDuct = pure () }

    contramapIO :: forall a b .
                    (a -> IO b)
                    -> WriteDuct b
                    -> WriteDuct a
    contramapIO f wd = wd { writeDuct = f >=> writeDuct wd }

