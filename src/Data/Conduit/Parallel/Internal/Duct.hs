{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleContexts    #-}
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
    writeDuct
) where

    import           Control.Concurrent.Classy
    import qualified Control.Monad.Catch        as Catch
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
    newtype Waiter m = Waiter {
        waiterTVar :: TVar (STM m) WaiterStatus
    }
    -- Classy STM doesn't allow us to use Eq on TVars.  This is mildly
    -- annoying.

    {-
    For debugging, we may want:
    data Waiter m = Waiter {
        waiterTVar :: TVar (STM m) WaiterStatus,
        waiterThreadId :: ThreadId m
    }
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
    data Status m a = Status {
                    sreaders :: Seq (Waiter m),
                    swriters :: Seq (Waiter m),
                    sclosing :: {-# UNPACK #-} !Bool,
                    scontents :: Contents a }

    -- | The read endpoint of a duct.
    data ReadDuct m a = ReadDuct { 
                            readDuct :: m (Maybe a),
                            closeReadDuct :: m (Maybe a) }

    instance Functor m => Functor (ReadDuct m) where
        fmap f rd = ReadDuct {
                        readDuct = fmap f <$> readDuct rd,
                        closeReadDuct = fmap f <$> closeReadDuct rd }

    -- | The write endpoint of a duct.
    data WriteDuct m a = WriteDuct {
                            writeDuct :: a -> m Open,
                            closeWriteDuct :: m () }

    instance Contravariant (WriteDuct m) where
        contramap f wd = wd { writeDuct = (writeDuct wd . f) }

    -- | Maybe-analog for whether an operation has completed or would block.
    data WouldBlock a =
        Complete a
        | WouldBlock

    -- | Maybe-analog for whether the duct is closed or not.
    --
    -- Note: I can tell the difference between WouldBlock (IsClosed a)
    -- and IsClosed (WouldBlock a).  With Maybe (Maybe a), I can't.
    data IsClosed a =
        IsOpen a
        | IsClosed

    type StatusM m a x = StateT (Status m a) (STM m) x


    data SLens m a x = SLens {
        view :: Status m a -> x,
        update :: x -> Status m a -> Status m a
    }

    readers :: forall a m . SLens m a (Seq (Waiter m))
    readers = SLens {
                view = sreaders,
                update = \x s -> s { sreaders = x } }

    writers :: forall a m . SLens m a (Seq (Waiter m))
    writers = SLens {
                view = swriters,
                update = \x s -> s { swriters = x } }

    contents :: forall a m . SLens m a (Contents a)
    contents = SLens {
                    view = scontents,
                    update = \x s -> s { scontents = x } }

    closing :: forall a m . SLens m a Bool
    closing = SLens {
                    view = sclosing,
                    update = \x s -> s { sclosing = x } }

    viewM :: Monad (STM m) => SLens m a x -> StatusM m a x
    viewM lens = view lens <$> get
    {-# INLINE viewM #-}

    give :: Monad (STM m) => SLens m a x -> x -> StatusM m a ()
    give lens val = modify (update lens val)

    mutate :: Monad (STM m) => SLens m a x -> (x -> x) -> StatusM m a ()
    mutate lens f = modify (\s -> update lens (f (view lens s)) s)

    addWaiter :: forall m a .
                    Monad (STM m)
                    => SLens m a (Seq (Waiter m))
                    -> Waiter m
                    -> StatusM m a ()
    addWaiter lens w = mutate lens (Seq.|> w)

    getQueueHead :: forall a m .
                        MonadSTM (STM m)
                        => SLens m a (Seq (Waiter m))
                        -> StatusM m a (Maybe (Waiter m))
    getQueueHead queue = do
            q :: Seq (Waiter m) <- viewM queue
            loop False q
        where
            loop :: Bool
                    -> Seq (Waiter m)
                    -> StatusM m a (Maybe (Waiter m))
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

            saveQueue :: Bool -> Seq (Waiter m) -> StatusM m a ()
            saveQueue False _ = pure ()
            saveQueue True  q = give queue q


    awaken :: forall a m . 
                MonadSTM (STM m)
                => SLens m a (Seq (Waiter m)) 
                -> StatusM m a ()
    awaken q = do
        r <- getQueueHead q
        case r of
            Nothing -> pure ()
            Just w  -> lift $ writeTVar (waiterTVar w) Awoken

    removeHead :: forall a m .
                    Monad (STM m)
                    => SLens m a (Seq (Waiter m))
                    -> StatusM m a (Waiter m)
    removeHead queue = do
        q :: Seq (Waiter m) <- viewM queue
        case Seq.viewl q of
            Seq.EmptyL  -> error "removeHead called on empty queue!"
            w Seq.:< ws -> do
                give queue ws
                pure w

    ifM :: forall m a . Monad m => m Bool -> m a -> m a -> m a
    ifM testM thenM elseM = do
        t <- testM
        if t
        then thenM
        else elseM

    withWaiter :: forall a b m .
                    (Catch.MonadCatch m
                    , Catch.MonadMask m
                    , MonadConc m)
                    => TVar (STM m) (IsClosed (Status m b))
                    -> (Waiter m -> m a)
                    -> m a
    withWaiter tvar go = Catch.bracketOnError makeWaiter doCleanUp go
        where
            makeWaiter :: m (Waiter m)
            makeWaiter = Waiter <$> newTVarConc Abandoned
                                -- <*> myThreadId

            doCleanUp :: Waiter m -> m ()
            doCleanUp waiter =
                atomically $ do
                    writeTVar (waiterTVar waiter) Abandoned
                    withOpen tvar () $ do
                        cnts <- viewM contents
                        case cnts of
                            Full _ -> awaken readers
                            Empty  -> ifM (viewM closing)
                                        (pure ()) 
                                        (awaken writers)
                        pure (Open, ())

    withOpen :: forall a b m .
                    MonadSTM (STM m)
                    => TVar (STM m) (IsClosed (Status m a))
                    -> b
                    -> StatusM m a (Open, b)
                    -> STM m b
    withOpen tvar onClosed onOpen = do
        ic :: IsClosed (Status m a) <- readTVar tvar
        case ic of
            IsClosed -> pure onClosed
            IsOpen s1 -> do
                ((open, b), s2) :: ((Open, b), Status m a)
                    <- runStateT onOpen s1 
                case open of
                    Open   -> writeTVar tvar (IsOpen s2)
                    Closed -> writeTVar tvar IsClosed
                pure b

    block :: forall a x m .
                MonadSTM (STM m)
                => SLens m a (Seq (Waiter m))
                -> Waiter m
                -> StatusM m a (Open, (WouldBlock x))
    block queue waiter = do
        lift $ writeTVar (waiterTVar waiter) Waiting
        addWaiter queue waiter
        pure (Open, WouldBlock)

    getInLine :: forall a x m .
                    MonadSTM (STM m)
                    => SLens m a (Seq (Waiter m))
                    -> Waiter m
                    -> StatusM m a (Open, WouldBlock x)
                    -> StatusM m a (Open, WouldBlock x)
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

    waitForWaiter :: forall a x m .
                        MonadSTM (STM m)
                        => SLens m a (Seq (Waiter m))
                        -> Waiter m
                        -> x
                        -> StatusM m a x
                        -> StatusM m a x
    waitForWaiter queue waiter onClosed act = do
        ws :: WaiterStatus <- lift $ readTVar (waiterTVar waiter)
        case ws of
            Waiting   -> lift retry     -- Block
            Abandoned -> pure onClosed
            Closing   -> pure onClosed
            Awoken    -> do
                -- We know we are on the queue, and we should be
                -- the head.  So we just remove the head element.
                _ <- removeHead queue
                -- We used to assert that the waiter we just removed
                -- was us.  Unfortunately, Classy STM doesn't define Eq
                -- for TVars, so we can't.  We just have to assume this
                -- is correct.
                act

    handleBlock :: forall x m .
                    MonadConc m
                    => (Waiter m -> STM m (WouldBlock x))
                    -> (Waiter m -> STM m x)
                    -> Waiter m
                    -> m x
    handleBlock preblock postblock waiter = do
        r <- atomically $ preblock waiter
        case r of
            Complete x -> pure x
            WouldBlock -> atomically $ postblock waiter

    setContents :: forall a m .
                    MonadSTM (STM m)
                    => Contents a
                    -> StatusM m a ()
    setContents cnts = do
        give contents cnts
        case cnts of
            Empty -> awaken writers
            Full _ -> awaken readers

    closeAll :: MonadSTM (STM m) => Seq (Waiter m) -> STM m ()
    closeAll q = 
        case Seq.viewl q of
            Seq.EmptyL  -> pure ()
            w Seq.:< ws -> do
                writeTVar (waiterTVar w) Closing
                closeAll ws

    doReadDuct :: forall a m .
                    MonadConc m
                    => TVar (STM m) (IsClosed (Status m a))
                    -> m (Maybe a)
    doReadDuct tvar = withWaiter tvar $ handleBlock preblock postblock
        where
            preblock :: Waiter m -> STM m (WouldBlock (Maybe a))
            preblock waiter = do
                -- Are we even open?  If not, return Nothing.
                withOpen tvar (Complete Nothing) $ do
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

            postblock :: Waiter m -> STM m (Maybe a)
            postblock waiter = 
                -- Make sure we are still open
                withOpen tvar Nothing $ do
                    -- wait to be woken up.
                    waitForWaiter readers waiter (Open, Nothing) $ do
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

            emptyContents :: forall r . r -> StatusM m a (Open, r)
            emptyContents r = do
                                -- The duct is not empty.  If we are closing,
                                -- close the duct.  Otherwise, empty the
                                -- contents, then return the value.
                                ifM (viewM closing)
                                    (pure (Closed, r))
                                    $ do
                                        setContents Empty
                                        pure (Open, r)

    doCloseReadDuct :: forall a m .
                        MonadConc m
                        => TVar (STM m) (IsClosed (Status m a))
                        -> m (Maybe a)
    doCloseReadDuct tvar = atomically $ do
        ms :: IsClosed (Status m a) <- readTVar tvar
        case ms of
            IsClosed -> pure Nothing
            IsOpen s -> do
                writeTVar tvar IsClosed
                closeAll (view readers s)
                closeAll (view writers s)
                pure $ case view contents s of
                            Empty -> Nothing
                            Full a -> Just a

    doWriteDuct :: forall a m .
                    MonadConc m
                    => TVar (STM m) (IsClosed (Status m a))
                    -> a
                    -> m Open
    doWriteDuct tvar val = withWaiter tvar $ handleBlock preblock postBlock
        where
            preblock :: Waiter m -> STM m (WouldBlock Open)
            preblock waiter = do
                -- Are we even open?  If not, return Closed.
                withOpen tvar (Complete Closed) $ do
                    -- Note: we can be closed for writing, even if we're
                    -- still open for reading.
                    ifM (viewM closing)
                        (pure (Open, (Complete Closed)))
                        $ do
                            getInLine writers waiter $ do
                            -- Look the contents of the duct.
                            cnts :: Contents a <- viewM contents
                            case cnts of
                                Empty  -> do
                                    -- The duct is empty.  Fill it.
                                    setContents (Full val)
                                    pure (Open, Complete Open)
                                Full _ -> do
                                    -- The duct is not empty.  Block until
                                    -- it is.
                                    block writers waiter

            postBlock :: Waiter m -> STM m Open
            postBlock waiter = 
                -- Make sure we are still open
                withOpen tvar Closed $ do
                    -- wait to be woken up.
                    waitForWaiter writers waiter (Closed, Closed) $ do
                        ifM (viewM closing)
                            -- Note: if the closing variable is True, we're
                            -- still open for reading.  So this is correct,
                            -- as weird as it looks.
                            (pure (Open, Closed))
                            $ do
                                -- Get the contents
                                cnts :: Contents a <- viewM contents
                                case cnts of
                                    Empty  -> do
                                        setContents (Full val)
                                        pure (Open, Open)
                                    Full _ -> do
                                        -- We should not have been woken up
                                        -- if the duct is full.
                                        error $ "writeDuct woken up on "
                                                ++ "full duct."


    doCloseWriteDuct :: forall a m .
                            MonadConc m
                            => TVar (STM m) (IsClosed (Status m a))
                            -> m ()
    doCloseWriteDuct tvar = atomically $ do
        ms :: IsClosed (Status m a) <- readTVar tvar
        case ms of
            IsClosed -> pure ()
            IsOpen s -> do
                case (view contents s) of
                    Empty -> do
                        writeTVar tvar IsClosed
                        closeAll (view readers s)
                        closeAll (view writers s)
                    Full _ -> do
                        let s2 = update writers Seq.empty s
                            s3 = update closing True s2
                        writeTVar tvar (IsOpen s3)
                        closeAll (view writers s)

    makeDuct :: forall a m .
                    MonadConc m
                    => TVar (STM m) (IsClosed (Status m a))
                    -> (ReadDuct m a, WriteDuct m a)
    makeDuct tvar = (rd, wd)
        where
            rd :: ReadDuct m a
            rd = ReadDuct {
                    readDuct = doReadDuct tvar,
                    closeReadDuct = doCloseReadDuct tvar }
            wd :: WriteDuct m a
            wd = WriteDuct {
                    writeDuct = doWriteDuct tvar,
                    closeWriteDuct = doCloseWriteDuct tvar }

    makeStatus :: forall a m . Contents a -> IsClosed (Status m a)
    makeStatus r = IsOpen $
                        Status {
                            sreaders = mempty,
                            swriters = mempty,
                            sclosing = False,
                            scontents = r }

    newDuct :: forall a m . MonadConc m => m (ReadDuct m a, WriteDuct m a)
    newDuct = makeDuct <$> newTVarConc (makeStatus Empty) 

    newFullDuct :: forall a m .
                    MonadConc m
                    => a
                    -> m (ReadDuct m a, WriteDuct m a)
    newFullDuct a = makeDuct <$> newTVarConc (makeStatus (Full a))

    newClosedDuct :: forall a m .
                        Applicative m
                        => m (ReadDuct m a, WriteDuct m a)
    newClosedDuct = pure (rd, wd)
        where
            rd :: ReadDuct m a
            rd = ReadDuct {
                    readDuct = pure Nothing,
                    closeReadDuct = pure Nothing }
            wd :: WriteDuct m a
            wd = WriteDuct {
                    writeDuct = (\_ -> pure Closed),
                    closeWriteDuct = pure () }

