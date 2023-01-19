{-# LANGUAGE DeriveGeneric       #-}
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

    import           Control.Concurrent.STM
    import qualified Control.Exception      as Ex
    import           Control.Monad.State
    import           Data.List.NonEmpty     (NonEmpty (..))
    import qualified Data.List.NonEmpty     as NE
    import           GHC.Generics           (Generic)

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
    --          - block on our waiter
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

    -- | A semi-optimized Banker's queue implementation.
    --
    -- This lets us reduce our dependency footprint.  This implementation
    -- is optimized for the very common cases of an empty queue and a
    -- queue holding a single value.
    data Queue a =
        QEmpty
        | QSingle a
        | QFull (NonEmpty a) (NonEmpty a)

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
        waiterThreadId :: ThreadId
    } deriving (Eq)
    -}

    -- | The current state of a Duct.
    --
    -- This used to be called the State, but then I wanted to use a StateT
    -- transformer, and the name clash caused sadness in the compiler.  So
    -- it got renamed.
    data Status a = Status {
                    readers :: Queue Waiter,
                    writers :: Queue Waiter,
                    current :: Maybe a }

    -- | The read endpoint of a duct.
    newtype ReadDuct a = ReadDuct { getReadTVar :: TVar (Maybe (Status a)) }

    -- | The write endpoint of a duct.
    newtype WriteDuct a = WriteDuct { getWriteTVar :: TVar (Maybe (Status a)) }

    -- | Which operation are we performing?
    --
    -- A bad idea, carried through to perfection.
    --
    -- Reads and writes are almost identical.  We take advantage of that
    -- plus the fact that we're unlikely to add new operations to eliminate
    -- a bunch of code duplication.
    --
    data Op = Read | Write deriving (Show)

    type StatusM a x = StateT (Bool, Status a) STM x

    newDuct :: forall a . IO (ReadDuct a, WriteDuct a)
    newDuct = do
            tvar <- newTVarIO (Just defaultStatus)
            pure (ReadDuct tvar, WriteDuct tvar)
        where
            defaultStatus :: Status a
            defaultStatus = Status {
                                readers = QEmpty,
                                writers = QEmpty,
                                current = Nothing }

    newFullDuct :: forall a . a -> IO (ReadDuct a, WriteDuct a)
    newFullDuct x = do
            tvar <- newTVarIO (Just defaultStatus)
            pure (ReadDuct tvar, WriteDuct tvar)
        where
            defaultStatus :: Status a
            defaultStatus = Status {
                                readers = QEmpty,
                                writers = QEmpty,
                                current = Just x }

    newClosedDuct :: forall a . IO (ReadDuct a, WriteDuct a)
    newClosedDuct = do
        tvar <- newTVarIO Nothing
        pure (ReadDuct tvar, WriteDuct tvar)

    closeDuct :: forall a .  TVar (Maybe (Status a)) -> IO (Maybe a)
    closeDuct tvar = join . atomically $ go
        where
            go :: STM (IO (Maybe a))
            go = do
                r <- readTVar tvar
                case r of
                    Nothing -> pure $ pure Nothing
                    Just st -> do
                        writeTVar tvar Nothing
                        pure $ closeQueue (readers st)
                                >> closeQueue (writers st)
                                >> pure (current st)

            closeQueue :: Queue Waiter -> IO ()
            closeQueue QEmpty                      = pure ()
            closeQueue (QSingle x)                 = closeWaiter x
            closeQueue (QFull (x :| xs) (y :| ys)) = do
                closeWaiter x
                mapM_ closeWaiter xs
                mapM_ closeWaiter (reverse ys)
                closeWaiter y

            closeWaiter :: Waiter -> IO ()
            closeWaiter waiter =
                atomically $ writeTVar (waiterTVar waiter) Closing

    closeReadDuct :: forall a . ReadDuct a -> IO (Maybe a)
    closeReadDuct = closeDuct . getReadTVar

    closeWriteDuct :: forall a . WriteDuct a -> IO (Maybe a)
    closeWriteDuct = closeDuct . getWriteTVar

    
    modifyDuct :: forall a b . 
                    Op
                    -> (Maybe a -> Maybe (Maybe a, b))
                    -> TVar (Maybe (Status a))
                    -> IO (Maybe b) -- Nothing == Closed
    modifyDuct op modCurrent tvar =
            -- The waiter starts life as abandoned, and we set it to
            -- waiting when we add it to the queue.  This way, the
            -- cleanup doesn't do unnecessary work.
            Ex.bracketOnError makeWaiter cleanUp go
        where
            makeWaiter :: IO Waiter
            makeWaiter =
                Waiter
                    <$> newTVarIO Abandoned
                    -- <*> myThreadId

            withStatus :: forall x . StatusM a x -> STM (Maybe x)
            withStatus act = do
                ms :: Maybe (Status a) <- readTVar tvar
                case ms of
                    Nothing    -> pure Nothing
                    Just st -> do
                        (x, (b, s)) <- runStateT act (False, st)
                        if b
                        then writeTVar tvar (Just s)
                        else pure ()
                        pure $ Just x

            go :: Waiter -> IO (Maybe b)
            go waiter = do
                r :: Maybe (Maybe b)
                    <- atomically $ withStatus (beforeBlocking waiter)
                case r of
                    -- We're closed
                    Nothing -> pure Nothing

                    -- We don't need to block
                    Just (Just b) -> pure (Just b)

                    -- We're blocking
                    Just Nothing -> do
                        t :: Maybe (Maybe b) <- atomically $ block waiter
                        case t of
                            -- We're closed
                            Nothing -> pure Nothing
                            -- We hit a logic error- we were woken up
                            -- but the current status wasn't correct.
                            -- So throw an exception.
                            -- See the comment down in blocking.
                            Just Nothing ->
                                Ex.assert False $
                                    error "Unreachable state reached."
                            -- We succeeded.
                            Just (Just b) ->
                                pure $ Just b

            purgeAbandons ::
                Op
                -> StatusM a (Queue Waiter, Maybe (Waiter, WaiterStatus))
            purgeAbandons op1 = do
                    q :: Queue Waiter <- getQueue op1
                    loop q False
                where
                    loop :: Queue Waiter
                                -> Bool
                                -> StatusM a (Queue Waiter,
                                                Maybe (Waiter, WaiterStatus))
                    loop q needWrite =
                        case qhead q of
                            Nothing     -> fini q needWrite Nothing
                            Just waiter -> do
                                s <- lift $ readTVar (waiterTVar waiter)
                                case s of
                                    Abandoned -> loop (qtail q) True
                                    _         -> fini q needWrite
                                                    (Just (waiter, s))

                    fini :: Queue Waiter
                                -> Bool
                                -> Maybe (Waiter, WaiterStatus)
                                -> StatusM a (Queue Waiter,
                                                Maybe (Waiter, WaiterStatus))
                    fini q needWrite status = do
                        if needWrite
                        then setQueue op1 q
                        else pure ()
                        pure (q, status)

            beforeBlocking :: Waiter -> StatusM a (Maybe b)
            beforeBlocking waiter = do
                (q :: Queue Waiter, _) <- purgeAbandons op
                if (qnull q)
                then do
                    -- No one else is waiting, so check the current value
                    curr <- getCurrent
                    case modCurrent curr of
                        -- If we don't have a value in the current, we need
                        -- to block
                        Nothing -> do
                            lift $ writeTVar (waiterTVar waiter) Waiting
                            setQueue op (enqueue q waiter)
                            pure Nothing
                        -- If we got a value, wake up the other side
                        Just (newCurrent, b) -> do
                            setCurrent newCurrent
                            ensureAwake (other op)
                            pure (Just b)
                else do
                    -- Other people are in queue, add ourselves
                    lift $ writeTVar (waiterTVar waiter) Waiting
                    setQueue op (enqueue q waiter)
                    pure Nothing

            block :: Waiter -> STM (Maybe (Maybe b))
            block waiter = do
                r <- readTVar (waiterTVar waiter)
                case r of
                    Waiting   -> retry        -- block
                    Abandoned -> pure Nothing -- This should not be possible.
                    Closing   -> pure Nothing
                    Awoken    -> do
                        -- Mark our waiter as abandoned- this is important to
                        -- prevent the cleanup routine from doing unnecessary
                        -- work if this transaction commits.
                        writeTVar (waiterTVar waiter) Abandoned
                        withStatus $ afterBlocking waiter

            afterBlocking :: Waiter -> StatusM a (Maybe b)
            afterBlocking _waiter = do
                -- remove ourselves from the queue
                q :: Queue Waiter <- getQueue op
                Ex.assert (Just _waiter == qhead q) $ pure ()
                setQueue op (qtail q)

                -- Get our value
                curr <- getCurrent
                case modCurrent curr of
                    -- This should never return Nothing.  This represents
                    -- a logic fail.  Normally, we'd throw an exception
                    -- right here, but we want the transaction to commit.
                    -- So we delay throwing the exception.
                    Nothing -> do
                        ensureAwake (other op)
                        pure Nothing
                    Just (newCurrent, b) -> do
                        setCurrent newCurrent
                        ensureAwake (other op)
                        pure (Just b)

            cleanUp :: Waiter -> IO ()
            cleanUp waiter = do
                s <- atomically $ do
                        s <- readTVar (waiterTVar waiter)
                        writeTVar (waiterTVar waiter) Abandoned
                        pure s
                case s of
                    Waiting   -> pure ()
                    -- Abandoned isn't an unusual value here- it means
                    -- either we haven't added ourselves to a queue
                    -- (yet), or we've dequeued ourselves and completed
                    -- the operation and then got hit with an async
                    -- exception.  In either case, cleanup is a no-op.
                    Abandoned -> pure ()
                    Closing   -> pure ()
                    Awoken    -> do
                        _ <- atomically . withStatus $ do
                                curr <- getCurrent
                                case curr of
                                    Nothing -> ensureAwake Write
                                    Just _  -> ensureAwake Read
                        pure ()

            ensureAwake :: Op -> StatusM a ()
            ensureAwake op1 = do
                (_, s :: Maybe (Waiter, WaiterStatus)) <- purgeAbandons op1

                case s of
                    Nothing                  -> pure ()
                    Just (waiter, Waiting)   -> do
                        lift $ writeTVar (waiterTVar waiter) Awoken
                        pure ()
                    Just (_,      Awoken)    -> pure ()
                    Just (_,      Abandoned) -> pure () -- Should never happen
                    Just (_,      Closing)   -> pure ()
 
            getQueue :: Op -> StatusM a (Queue Waiter)
            getQueue op1 = do
                (_, s) <- get
                pure $ case op1 of
                            Read  -> readers s
                            Write -> writers s

            setQueue :: Op -> Queue Waiter -> StatusM a ()
            setQueue op1 q = do
                (_, s) <- get
                let s2 :: Status a
                    s2 = case op1 of
                            Read  -> s { readers = q }
                            Write -> s { writers = q }
                put (True, s2)

            getCurrent :: StatusM a (Maybe a)
            getCurrent = do
                (_, s) <- get
                pure $ current s

            setCurrent :: Maybe a -> StatusM a ()
            setCurrent newCurrent = do
                (_, s) <- get
                put (True, s { current = newCurrent })

            other :: Op -> Op
            other Read  = Write
            other Write = Read


    readDuct :: forall a . ReadDuct a -> IO (Maybe a)
    readDuct = modifyDuct Read f . getReadTVar
        where
            f :: Maybe a -> Maybe (Maybe a, a)
            f Nothing = Nothing
            f (Just a) = Just (Nothing, a)


    writeDuct :: forall a . WriteDuct a -> a -> IO Open
    writeDuct duct a = fixup <$> modifyDuct Write f (getWriteTVar duct)
        where
            f :: Maybe a -> Maybe (Maybe a, ())
            f Nothing  = Just (Just a, ())
            f (Just _) = Nothing

            fixup :: Maybe () -> Open
            fixup Nothing   = Closed
            fixup (Just ()) = Open

    enqueue :: Queue a -> a -> Queue a
    enqueue QEmpty        x = QSingle x
    enqueue (QSingle x)   y = QFull (NE.singleton x) (NE.singleton y)
    enqueue (QFull hs ts) x = QFull hs (NE.cons x ts)

    qnull :: Queue a -> Bool
    qnull QEmpty      = True
    qnull (QSingle _) = False
    qnull (QFull _ _) = False

    qhead :: Queue a -> Maybe a
    qhead QEmpty             = Nothing
    qhead (QSingle x)        = Just x
    qhead (QFull (x :| _) _) = Just x

    qtail :: Queue a -> Queue a
    qtail QEmpty                              = QEmpty
    qtail (QSingle _)                         = QEmpty
    qtail (QFull (_ :| []) (x  :| xs))        =
        case reverse xs of
            []     -> QSingle x
            (y:ys) -> QFull (y :| ys) (x :| [])
    qtail (QFull (_ :| (x : xs)) ts)          = QFull (x :| xs) ts

    -- Needed for debugging.
    {-
    qelems :: Queue a -> [ a ]
    qelems QEmpty = []
    qelems (QSingle x) = [ x ]
    qelems (QFull (x :| xs) (y :| ys)) = x : (xs ++ reverse ys ++ [y])
    -}


