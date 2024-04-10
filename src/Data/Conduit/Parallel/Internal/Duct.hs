{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Duct
-- Description : Ducts (aka Closable MVars)
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
-- = Use "Worker" and "Control" Instead.
--
-- Rather than directly using this module, use the
-- "Data.Conduit.Parallel.Internal.Control" and
-- "Data.Conduit.Parallel.Internal.Worker" modules instead instead.  
-- Those modules wrap these functions in a way more useful for
-- Parallel Conduits code.  This module may then, one day, be spun off
-- into it's own library.
--
-- = Introduction
--
-- This module implements Ducts (aka Closable MVars).  A duct is a one
-- element channel between threads.  It works much like an MVar does,
-- in the reading the duct when it's empty blocks, as does writting to
-- the duct when it's full. 
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
-- can be read from) and a WriteDuct endpoint (that can be written to).
--
module Data.Conduit.Parallel.Internal.Duct(
    Open(..),
    ReadDuct,
    WriteDuct,
    Duct,
    newDuct,
    newFullDuct,
    newClosedDuct,
    withReadDuct,
    withWriteDuct,
    addReadOpens,
    addWriteOpens,
    contramapIO
) where

    import           Control.Concurrent.STM
    import           Control.Exception          (assert, bracket)
    import           Control.Monad.State
    import           Data.Functor.Contravariant
    import           Data.Sequence              (Seq)
    import qualified Data.Sequence              as Seq
    import           GHC.Generics               (Generic)
    import           UnliftIO                   (MonadUnliftIO)
    import qualified UnliftIO

    -- So, a word of warning before we dig in.  Yeah, this module is
    -- documented.  Probably over-documented.  But here's the thing:
    -- this is the core module for this whole library.  If this doesn't
    -- work, nothing works- and it probably fails in hard to debug ways.
    -- So we document very heavily.  And use types as much as possible
    -- to gaurantee correctness.  And break things up into simple chunks,
    -- depending upon the compiler to re-integrate them and optimize them.
    -- And have a bunch of unit tests.  It's a case of belt *and* suspenders,
    -- and duct tape and super glue and staples and ...
    --
    -- 
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
    --
    -- ** Closing
    --
    -- The fact that the duct can hold a value causes some interesting
    -- consequences for closing the duct: what do you do when you are
    -- closing a duct holding a value?  Closing the read end is easy-
    -- doing a close implicitly does a non-blocking read.  If the duct
    -- is holding a value, closing the read end returns the value.  The
    -- closing code is then responsible for handling that last value.
    -- It can discard it or handle it however, as it chooses.
    --
    -- Closing the write end of the duct is somewhat more compilicated.
    -- You can't return the value on the write.  In addition to the
    -- writting code not knowing how to properly handle the last value,
    -- the writing code may not even know the true type of the duct.
    -- The write end may have had `contramap` called on it, so the
    -- writer is sending values of type a, but the last value in the
    -- duct is of type b.
    --
    -- The solution for this is to have two different sorts of "closed"
    -- the duct can be.  If the duct is fully closed, this means that
    -- both reads and writes will fail.  This is the normal definition
    -- of closed.  But a duct can also be only write-closed.  With a
    -- write-closed duct, writes will fail, but reads will still
    -- succeed.  Closing the read end of a duct will always fully close
    -- the duct (returning the last element if it exists).  If the
    -- duct is empty when the write end is closed, then the duct will
    -- be fully closed.  But if the duct contains an element when the
    -- write end is closed, the duct will be only write-closed.  The
    -- next read will succeed, and in doing so fully close the duct.
    --

    -- | Boolean-analog for whether the Duct is open or closed.
    --
    -- Note that Ducts can be closed, which means that the function
    -- given out by `withWriteDuct` can fail if the Duct is closed.  So
    -- instead of returning () like putMVar does, the function returns
    -- a Open value.  A value of Open means the operation succeeded,
    -- a value of Closed means it failed.
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
        Full a      -- ^ The duct is full (contains a value)
        | Empty     -- ^ The duct is empty
    -- This used to be called the State, but then I wanted to use a StateT
    -- transformer, and the name clash caused sadness in the compiler.  So
    -- it got renamed.

    -- | The current state of a Duct.
    --
    data Status a = Status {
                        sReadersQueue :: Seq Waiter,
                        sWritersQueue :: Seq Waiter,
                        sWriteClosed :: Bool,
                        sContents :: Contents a }
    -- It is possible for there to be both readers and writers queued up.
    -- This is because we depend upon the threads to remove themselves
    -- from the queues- so, for example, a reader could empty the duct,
    -- waking up a writer.  But before the writer can wake up, refill the
    -- duct, and remove itself from the queue, more readers show up.
    --
    -- This is generally a short-lived situation.  But "short-lived"
    -- does not mean "impossible".
    --
    -- Also note that the duct can be partially closed- closed for writes,
    -- but still open for reads (as it holds a value).

    -- | The read endpoint of a duct.
    data ReadDuct a = ReadDuct { 
                            readAddOpens :: Int -> IO (),
                            readDuct :: Maybe (IO ()) -> IO (Maybe a),
                            closeReadDuct :: IO () }

    instance Functor ReadDuct where
        fmap f rd = rd { readDuct = \mr -> fmap f <$> readDuct rd mr }

    -- | The write endpoint of a duct.
    data WriteDuct a = WriteDuct {
                            writeAddOpens :: Int -> IO (),
                            writeDuct :: Maybe (IO ()) -> a -> IO Open,
                            closeWriteDuct :: IO () }

    instance Contravariant WriteDuct where
        contramap f wd = wd { writeDuct = \mr -> writeDuct wd mr . f }

    -- | A convience type for creating a duct.
    --
    -- This allows us to do:
    --
    -- @
    --      (rd, wd) :: Duct Int <- newDuct
    -- @
    --
    -- rather than the more clunky:
    --
    -- @
    --      (rd, wd) :: (ReadDuct Int, WriteDuct Int) <- newDuct
    -- @
    --
    type Duct a = (ReadDuct a, WriteDuct a)

    -- | Maybe-analog for whether an operation has completed or would block.
    data WouldBlock a =
        Complete a
        | WouldBlock

    -- | Maybe-analog for whether the duct is closed or not.
    data IsClosed a =
        IsOpen a
        | IsClosed

    -- | TVar type holding a status (and is maybe closed).
    type StatusTVar a = TVar (IsClosed (Status a))

    -- | Monad for updating the Status.
    --
    -- It simplifies the code to move the duct status into a StateT
    -- transformer, rather than have readTVars and writeTvars all
    -- over the place.
    --
    -- Note that the base monad of this status is STM, not IO!
    type StatusM a = StateT (IsClosed (Status a)) STM

    -- | Test if the duct is closed.
    isClosed :: forall a . StatusM a Bool
    isClosed = do
        s <- get
        pure $ case s of
                IsClosed -> True
                IsOpen _ -> False

    -- | A Redneck lens.
    --
    -- Using lenses here cleans up the code, but I don't want to depend
    -- on the real lens library.  So it's time to break out the duct tape
    -- and bailing wire for things that move and shouldn't, and a hammer
    -- and WD40 for thats that don't move and should.
    --
    data SLens a x = SLens {
        view :: Status a -> x,
        update :: x -> Status a -> Status a
    }

    -- | Lens to access the readers queue.
    readers :: forall a . SLens a (Seq Waiter)
    readers = SLens {
                view = sReadersQueue,
                update = \x s -> s { sReadersQueue = x } }

    -- | Lens to access the writers queue.
    writers :: forall a . SLens a (Seq Waiter)
    writers = SLens {
                view = sWritersQueue,
                update = \x s -> s { sWritersQueue = x } }

    -- | Lens to access the current contents of the duct.
    contents :: forall a . SLens a (Contents a)
    contents = SLens {
                    view = sContents,
                    update = \x s -> s { sContents = x } }

    -- | Lens to test if the duct is write closed.
    writeClosed :: forall a . SLens a Bool
    writeClosed = SLens {
                    view = sWriteClosed,
                    update = \x s -> s { sWriteClosed = x } }

    -- | Read the given lens on the current duct State.
    --
    -- This is a combination of view (the lens) and get (the state).
    --
    -- This fails is the duct is fully closed.
    viewM :: SLens a x -> StatusM a x
    viewM lens = do
        r <- get
        case r of
            IsClosed -> error "viewM on closed duct!"
            IsOpen s -> pure $ view lens s

    -- | Write the given lens on the current duct State.
    --
    -- This is a combination of update (the lens) and modify (the state).
    --
    -- If the duct is fully closed, this is a no-op.
    --
    give :: SLens a x -> x -> StatusM a ()
    give lens val =
        modify (\case
                    IsClosed -> IsClosed
                    IsOpen s -> IsOpen $ update lens val s)

    -- | Apply a given function to the value focued on by the lens in the
    -- current State.
    --
    -- This is a combination of update and view (for the lens) and
    -- modify (for the state).
    --
    -- If the duct is fully closed, this is a no-op.
    mutate :: SLens a x -> (x -> x) -> StatusM a ()
    mutate lens f =
        modify (\case
                    IsClosed -> IsClosed
                    IsOpen s -> IsOpen $ update lens (f (view lens s)) s)

    -- | Force the duct to be fully closed.
    closeStatusM :: StatusM a ()
    closeStatusM = put IsClosed

    -- | Add a waiter to a given queue (lens).
    addWaiter :: forall a .
                    SLens a (Seq Waiter)
                    -> Waiter
                    -> StatusM a ()
    addWaiter lens w = mutate lens (Seq.|> w)

    -- | Get the head element of a given waiter queue.
    --
    -- This will discard waiters that this function discards waiters
    -- that have been abandoned.
    --
    -- Returns Nothing if the queue is empty (or all the waiters in the
    -- queue were abandoned).  Otherwise returns Just the head waiter.
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


    -- | Awake the head waiter in the given queue.
    --
    -- If there are no non-abandoned waiters in the queue (i.e.
    -- getQueueHead returns Nothing), then no waiter is woken up.
    --
    awaken :: forall a .  SLens a (Seq Waiter) -> StatusM a ()
    awaken q = do
        r <- getQueueHead q
        case r of
            Nothing -> pure ()
            Just w  -> lift $ writeTVar (waiterTVar w) Awoken

    -- | Remove the first waiter in the queue.
    --
    -- Note, this function does NOT discard abandoned waiters.  The
    -- assumption is we are removing ourselves from the queue, and thus
    -- are not abandoned.
    --
    removeHead :: forall a .
                    SLens a (Seq Waiter)
                    -> Waiter   -- ^ The waiter we expect to be the
                                -- first element.
                    -> StatusM a ()
    removeHead queue me = do
        q :: Seq Waiter <- viewM queue
        case Seq.viewl q of
            Seq.EmptyL  -> error "removeHead called on empty queue!"
            h Seq.:< ws ->
                assert (me == h) $ do
                    give queue ws
                    pure ()

    -- | An if where the test and both branches are monads.
    --
    -- Mildly suprised this doesn't already exist.  I may have just
    -- missed it.
    --
    ifM :: forall m a . Monad m => m Bool -> m a -> m a -> m a
    ifM testM thenM elseM = do
        t <- testM
        if t
        then thenM
        else elseM

    -- | Set up to wait on a waiter.
    --
    -- This function sets the waiter to be in state waiting,
    -- adds the waiter to the given queue, and returns WouldBlock.
    block :: forall a x .
                SLens a (Seq Waiter)
                -> Waiter
                -> StatusM a (WouldBlock x)
    block queue waiter = do
        lift $ writeTVar (waiterTVar waiter) Waiting
        addWaiter queue waiter
        pure WouldBlock

    -- | Enter a queue, if the queue is not empty.
    --
    -- For both reading and writing, if the associated queue is not
    -- empty, we simply want to add ourselves to the queue and block.
    -- We only want to go on if the queue is empty.
    getInLine :: forall a x .
                    SLens a (Seq Waiter)
                    -- ^ Queue to enter if it's not empty
                    -> Waiter
                    -- ^ Waiter to add to the queue
                    -> StatusM a (WouldBlock x)
                    -- ^ what to do if the queue is empty
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

    -- | The common structure of both the read and write functions.
    --
    -- Both the read and write functions have the same basic structure-
    -- we have a pre-block transaction that either returns without
    -- needing to block (when it returns Complete), or sets up
    -- to block (when it returns WouldBlock).  
    --
    -- If the pre-block transaction returns WouldBlock, we then start
    -- a second transaction.  This second transaction blocks on the
    -- waiter until it's the head of the queue, and then updates the
    -- duct and returns the correct value.
    --
    common :: forall x a .
                    Maybe (IO ())
                    -- ^ The IO action to perform on blocking.
                    -> StatusTVar a
                    -- ^ The TVar holding the duct's status.
                    -> x
                    -- ^ The value to return if the waiter is signalled
                    -- that the duct is closed.
                    -> (Waiter -> StatusM a (WouldBlock x))
                    -- ^ The pre-block transaction.
                    -> (Waiter -> StatusM a x)
                    -- ^ The blocking transaction.
                    -> IO x
    common onBlock tvar onClosed preblock postblock = 
            bracket makeWaiter doCleanUp go
        where
            go :: Waiter -> IO x
            go waiter = do
                r <- atomically . runStatusM $ preblock waiter
                case r of
                    Complete x -> pure x
                    WouldBlock -> do
                        case onBlock of
                            Nothing -> pure ()
                            Just act -> act
                        atomically $ waitForWaiter waiter 

            makeWaiter :: IO Waiter
            makeWaiter = Waiter <$> newTVarIO Abandoned
                            -- <*> myThreadId

            doCleanUp :: Waiter -> IO ()
            doCleanUp waiter =
                atomically $ do
                    writeTVar (waiterTVar waiter) Abandoned
                    runStatusM $ do
                        ifM isClosed (pure ()) $ do
                            cnts <- viewM contents
                            case cnts of
                                Full _ -> awaken readers
                                Empty  -> ifM (viewM writeClosed)
                                            (pure ()) 
                                            (awaken writers)

            runStatusM :: forall y .
                            StatusM a y
                            -> STM y
            runStatusM act = do
                s1 :: IsClosed (Status a) <- readTVar tvar
                (y, s2) :: (y, IsClosed (Status a)) <- runStateT act s1
                writeTVar tvar s2
                pure y

            waitForWaiter :: Waiter -> STM x
            waitForWaiter waiter = do
                ws :: WaiterStatus <- readTVar (waiterTVar waiter)
                case ws of
                    Waiting   -> retry -- Block
                    Abandoned -> pure onClosed
                    Closing   -> pure onClosed
                    Awoken    -> runStatusM $ postblock waiter

    -- | Set the contents of the duct.
    setContents :: forall a .
                    Contents a
                    -> StatusM a ()
    setContents cnts = do
        give contents cnts
        case cnts of
            Empty -> awaken writers
            Full _ -> awaken readers

    -- | Signal all the waiters in a queue that the duct is not closed.
    closeAll :: Seq Waiter -> STM ()
    closeAll = mapM_ go 
        where
            go :: Waiter -> STM ()
            go w = writeTVar (waiterTVar w) Closing

    -- | Read a value from the duct.
    doReadDuct :: forall a .
                    StatusTVar a
                    -- ^ The Duct's TVar.
                    -> Maybe (IO ())
                    -- ^ The IO action to take before blocking.
                    --
                    -- This is used for testing the Duct library- in
                    -- normal use, it should be Nothing.
                    -> IO (Maybe a)
    doReadDuct tvar onBlock = common onBlock tvar Nothing preblock postblock
        where
            preblock :: Waiter -> StatusM a (WouldBlock (Maybe a))
            preblock waiter = do
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

            postblock :: Waiter -> StatusM a (Maybe a)
            postblock waiter = do
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
                ifM (viewM writeClosed)
                    closeStatusM
                    $ setContents Empty
                pure r

    -- | Decrement the opens count of the duct.
    decrementCount :: TVar Int -> IO () -> IO ()
    decrementCount cvar doClose = do
        b <- atomically $ do
                cnt <- readTVar cvar
                if (cnt <= 0)
                then error "Extra closes!"
                else do
                    let !c2 = cnt - 1
                    writeTVar cvar c2
                    pure $ c2 == 0
        when b $ doClose

    -- | Close the read duct.
    doCloseReadDuct :: forall a .  StatusTVar a -> IO ()
    doCloseReadDuct tvar = atomically $ do
        ms :: IsClosed (Status a) <- readTVar tvar
        case ms of
            IsClosed -> pure ()
            IsOpen s -> do
                writeTVar tvar IsClosed
                closeAll (view readers s)
                closeAll (view writers s)
                pure ()

    -- | Write a value to the write duct.
    doWriteDuct :: forall a .
                    StatusTVar a
                    -> Maybe (IO ())
                    -> a
                    -> IO Open
    doWriteDuct tvar onBlock val = common onBlock tvar Closed preblock postblock
        where
            preblock :: Waiter -> StatusM a (WouldBlock Open)
            preblock waiter = do
                -- Are we even open?  If not, return Closed.
                ifM isClosed (pure (Complete Closed)) $ do
                    -- Note: we can be closed for writing, even if we're
                    -- still open for reading.
                    ifM (viewM writeClosed) (pure (Complete Closed)) $ do
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


            postblock :: Waiter -> StatusM a Open
            postblock waiter = do
                -- Make sure we are still open
                ifM isClosed (pure Closed) $ do
                    ifM (viewM writeClosed) (pure Closed) $ do
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


    -- | Close a write duct.
    doCloseWriteDuct :: forall a . StatusTVar a -> IO ()
    doCloseWriteDuct tvar = atomically $ do
        ms :: IsClosed (Status a) <- readTVar tvar
        case ms of
            IsClosed -> pure ()
            IsOpen s -> do
                -- Note that closing a write duct does not necessarily
                -- close the associated read side, if there is still
                -- a value in the duct (i.e. the next read will still
                -- succeed).
                case view contents s of
                    Empty -> do
                        writeTVar tvar IsClosed
                        closeAll (view readers s)
                        closeAll (view writers s)
                    Full _ -> do
                        let s2 = update writers Seq.empty s
                            s3 = update writeClosed True s2
                        writeTVar tvar (IsOpen s3)
                        closeAll (view writers s)


    -- | Increment the opens count.
    addOpens :: TVar Int -> Int -> IO ()
    addOpens cvar cnt
        | cnt <= 0  = error "addOpens: non-positive count!"
        | otherwise = atomically $ do
            c <- readTVar cvar
            let !c2 = c + cnt
            writeTVar cvar c2

    -- | Create a duct given a Contents.
    --
    -- This is the common code between `newDuct` and `newFullDuct`.
    makeDuct :: forall a . Contents a -> IO (Duct a)
    makeDuct cnts = do
        let makeStatus :: IsClosed (Status a)
            makeStatus = IsOpen $
                        Status {
                            sReadersQueue = mempty,
                            sWritersQueue = mempty,
                            sWriteClosed = False,
                            sContents = cnts }
        tvar <- newTVarIO makeStatus
        rcnt <- newTVarIO 1
        wcnt <- newTVarIO 1
        let rd :: ReadDuct a
            rd = ReadDuct {
                    readAddOpens = addOpens rcnt,
                    readDuct = doReadDuct tvar,
                    closeReadDuct = decrementCount rcnt
                                        (doCloseReadDuct tvar) }
            wd :: WriteDuct a
            wd = WriteDuct {
                    writeAddOpens = addOpens wcnt,
                    writeDuct = doWriteDuct tvar,
                    closeWriteDuct = decrementCount wcnt
                                        (doCloseWriteDuct tvar) }
        pure (rd, wd)


    -- | Create a new empty Duct.
    newDuct :: forall a .  IO (Duct a)
    newDuct = makeDuct Empty

    -- | Create a new duct that already holds a value.
    newFullDuct :: forall a .  a -> IO (Duct a)
    newFullDuct a = makeDuct $ Full a

    -- | Create a new closed duct.
    --
    -- Both read and write operations on the created duct will fail
    -- immediately.  Used to create the end caps for sources and sinks
    -- (ParConduit segments that should never read their inputs (sources)
    -- or write to their outputs (sinks)).
    --
    newClosedDuct :: forall a . Duct a
    newClosedDuct = (rd, wd)
        where
            rd :: ReadDuct a
            rd = ReadDuct {
                    readAddOpens = \_ -> pure (),
                    readDuct = \_ -> pure Nothing,
                    closeReadDuct = pure () }
            wd :: WriteDuct a
            wd = WriteDuct {
                    writeAddOpens = \_ -> pure (),
                    writeDuct = \_ -> const (pure Closed),
                    closeWriteDuct = pure () }

    -- | Contramap a write duct with a function that executes in IO.
    --
    -- Note that the computation is performed in whatever thread
    -- is writing to the write duct.  As such, this is not exported
    -- to the wider world.  It mainly exists so we can force
    -- the values being written to head normal form with a combination
    -- of `Control.Exception.evaluate` and `Control.DeepSeq.force`.
    contramapIO :: forall a b .
                    (a -> IO b)
                    -> WriteDuct b
                    -> WriteDuct a
    contramapIO f wd = wd { writeDuct = \mr -> f >=> writeDuct wd mr }

    -- | Allow reading from a ReadDuct.
    --
    -- Converts a ReadDuct into a function that reads values from that
    -- read duct.  This function uses the standard Haskell with*
    -- pattern for wrapping `Control.Exception.bracket`.  When the
    -- wrapped computation exits (either via normal value return or
    -- due to an exception), the open count of the read duct is
    -- decremented.  If it drops to zero, then the read is closed.
    -- 
    -- This function is designed to work with the ContT transformer.
    --
    -- This function takes an extra IO action, which is performed if
    -- and when the function blocks.  This is used for testing-
    -- normally it is Nothing.
    --
    withReadDuct :: forall a b m .
                        MonadUnliftIO m
                        => ReadDuct a
                        -> Maybe (IO ()) -- ^ The IO operation to perform
                                         -- on blocking (used for testing).
                        -> (IO (Maybe a) -> m b)
                        -> m b
    withReadDuct rd mio act =
        UnliftIO.finally
            (act (readDuct rd mio))
            (liftIO (closeReadDuct rd))

    -- | Allow writing to a WriteDuct.
    --
    -- Converts a WriteDuct into a function that writes values to that
    -- write duct.  This function uses the standard Haskell with*
    -- pattern for wrapping `Control.Exception.bracket`.  When the
    -- wrapped computation exits (either via normal value return or
    -- due to an exception), the open count of the write duct is
    -- decremented.  If it drops to zero, then the duct is closed.
    -- 
    -- This function is designed to work with the ContT transformer.
    --
    -- This function takes an extra IO action, which is performed if
    -- and when the function blocks.  This is used for testing-
    -- normally it is Nothing.
    --
    withWriteDuct :: forall a b m .
                        MonadUnliftIO m
                        => WriteDuct a
                        -> Maybe (IO ()) -- ^ The IO operation to perform
                                         -- on blocking (used for testing).
                        -> ((a -> IO Open) -> m b)
                        -> m b
    withWriteDuct wd mio act =
        UnliftIO.finally
            (act (writeDuct wd mio))
            (liftIO (closeWriteDuct wd))

    -- | Add opens to a read duct.
    --
    -- Read ducts have an open count associated with them.  When multiple
    -- threads are expected to share a read duct (for example, in
    -- `Data.Conduit.Parallel.Internal.Parallel.parallel`), you can add
    -- to the count of opens expected. This way, the last open to exit
    -- closes the read duct, not the first.
    --
    -- Note that read ducts start with an open count of 1.  So, if you
    -- are going to have N threads use the read duct, you need to
    -- add N-1 extra opens.
    --
    addReadOpens :: forall a . ReadDuct a -> Int -> IO ()
    addReadOpens rd n
        | n <= 0    = error "addReadOpens: non-positive count!"
        | otherwise = readAddOpens rd n

    -- | Add opens to a write duct.
    --
    -- Write ducts have an open count associated with them.  When multiple
    -- threads are expected to share a write duct (for example, in
    -- `Data.Conduit.parallel`), you can add to the count of opens expected.
    -- This way, the last open to exit closes the write duct, not the first.
    --
    -- Note that write ducts start with an open count of 1.  So, if you
    -- are going to have N threads use the write duct, you need to
    -- add N-1 extra opens.
    --
    addWriteOpens :: forall a . WriteDuct a -> Int -> IO ()
    addWriteOpens wd n
        | n <= 0    = error "addWriteOpens: non-positive count!"
        | otherwise = writeAddOpens wd n

