{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DuctTest(
    tests
) where

    import           Control.Concurrent.Async
    import qualified Control.Concurrent.MVar             as MVar
    import qualified Control.Exception.Lifted            as Ex
    import           Control.Monad.Cont
    import           Control.Monad.Except
    import           Control.Monad.State
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.IORef
    import           Test.HUnit

    tests :: Test
    tests = TestLabel "Duct Tests" $
                TestList [
                    testReadFull,
                    testReadClosed1,
                    testReadClosed2,
                    testReadClosed3,
                    testReadClosed4,
                    testCloseRead,
                    testWriteEmpty,
                    testWriteClosed1,
                    testWriteClosed2,
                    testWriteClosed3,
                    testReadBlocks,
                    testReadQueues,
                    testWriteBlocks,
                    testWriteQueues, 
                    testAbandonRead,
                    testAbandonWrite,
                    testCloseReads1,
                    testCloseReads2,
                    testCloseWrites1,
                    testCloseWrites2
                ]

    -- | The monad we run tests in.
    --
    -- This is just the ContT monad, allowing us to chain our withAsync
    -- calls, and clean up all the spawned threads when we're done with
    -- the test.
    --
    -- A common test will look like:
    --
    -- @
    --    myTest :: Test
    --    myTest = runTestM "MyTest" $ do
    --          v <- liftIO $ ...
    --          r1 <- waitForBlock $ spawn $ thread1 v
    --          r2 <- noWaitForBlock $ spawn $ thread2 v
    --          pure $ r1 && r2
    --      where
    --          thread1 :: whatever -> ThreadM Bool
    --          thread1 = ...
    --
    --          thread2 :: whatever -> ThreadM Bool
    --          thread2 = ...
    -- @
    --
    type TestM a = forall r . ContT r IO a

    -- | The monad we run in test threads
    --
    -- This allows us to short-circuit on a failed test, and
    -- have an option action to perform when the thread will
    -- block.
    --
    type ThreadM a = ExceptT Bool (StateT (Maybe (IO ())) IO) a

    -- | A known exception we can throw at threads.
    data TestException = TestException deriving (Show)

    instance Ex.Exception TestException where

    -- | Create a Test from a TestM
    --
    -- This does not create a label for the test, and is occassionally
    -- useful.
    runTestMBase :: Assertable a => TestM a -> Test
    runTestMBase act = TestCase $ do
                        a <- runContT act pure
                        assert a

    -- | Create a Test from a TestM with a label.
    --
    -- We generally want to label our individual tests.
    --
    runTestM :: Assertable a => String -> TestM a -> Test
    runTestM label act = TestLabel label $ runTestMBase act

    -- | Create a test that only needs a single thread.
    --
    -- Note that the thread can not block.
    --
    runSingleThread :: String -> ThreadM () -> Test
    runSingleThread label act =
        TestLabel label $ TestCase $ do
            let act1 :: StateT (Maybe (IO ())) IO (Either Bool ())
                act1 = runExceptT act

                act2 :: IO (Either Bool ())
                act2 = evalStateT act1 Nothing

            ebu :: Either Bool () <- act2
            case ebu of
                Left b   -> assert b
                Right () -> assert True

    -- | Wait for a block before continuing.
    --
    -- This function is intended to wrap `spawn`, causing the `TestM`
    -- code to wait for the newly spawned thread to block before
    -- continuing.
    waitForBlock :: (Maybe (IO ()) -> TestM a) -> TestM a
    waitForBlock go = do
        mvar <- liftIO $ MVar.newEmptyMVar
        r <- go (Just (MVar.putMVar mvar ()))
        _ <- liftIO $ MVar.takeMVar mvar
        pure r

    -- | Do not wait for a block before continuing.
    --
    -- Like `waitForBlock`, this function is intended to wrap `spawn`.
    -- But unlike the aforementioned function, this function does not
    -- wait for a block before continuing.  It is used for test threads
    -- which do not block.
    --
    noWaitForBlock :: (Maybe (IO ()) -> TestM a) -> TestM a
    noWaitForBlock spwn = spwn Nothing

    -- | Spawn a test thread (run a `ThreadM` monad).
    --
    -- This function is intended to be wrapped with either
    -- `waitForBlock` or `noWaitForBlock`.  It spawns a new
    -- thread, and runs the `ThreadM` monad in it.
    --
    spawn :: ThreadM () -> Maybe (IO ()) -> TestM (Async Bool)
    spawn act onBlock = ContT go
        where
            go :: forall r . (Async Bool -> IO r) -> IO r
            go f = do
                withAsync (runThreadM act onBlock) $ \asy -> do
                    link asy
                    r <- f asy
                    a <- wait asy
                    assert a
                    pure r

            runThreadM :: ThreadM () -> Maybe (IO ()) -> IO Bool
            runThreadM a onBlck = do
                let a1 :: StateT (Maybe (IO ())) IO (Either Bool ())
                    a1 = runExceptT a

                    a2 :: IO (Either Bool (), Maybe (IO ()))
                    a2 = runStateT a1 onBlck

                (ebb, doneBlock) :: (Either Bool (), Maybe (IO ())) <- a2

                -- Ensure the onBlock action has been performed.
                case doneBlock of
                    Nothing   -> pure ()
                    Just blck -> blck

                pure $ case ebb of
                            Left  b -> b
                            Right () -> True


    -- | Lift an IO action taking a onBlock value to a `ThreadM`.
    --
    -- This is intended to be used by `doRead` and `doWrite`.  If
    -- the underlying IO action doesn't need or use the onBlock
    -- action, then just use liftIO.
    withBlock :: forall a . (Maybe (IO ()) -> IO a) -> ThreadM a
    withBlock op = do
            s <- get
            case s of
                Nothing      -> liftIO $ op Nothing
                Just onBlck -> do
                    (r, s2) <- liftIO $ do
                        ref <- newIORef (Just onBlck)
                        r <- op (Just (updateRef ref))
                        s2 <- readIORef ref
                        pure (r, s2)
                    put s2
                    pure r
        where
            updateRef :: IORef (Maybe (IO ())) -> IO ()
            updateRef ref = do
                s3 <- readIORef ref
                case s3 of
                    Nothing -> pure ()
                    Just ob -> do
                        ob
                        writeIORef ref Nothing


    {-
    -- | Fake a block (executing the onBlock action if one exists).
    --
    -- Not sure if this is useful or not.
    pseudoBlock :: ThreadM ()
    pseudoBlock = do
        s <- get
        case s of
            Nothing -> pure ()
            Just op -> do
                liftIO op
                set Nothing
    -}

    -- | Pseudo-assert for ThreadM.
    attest :: Bool -> ThreadM ()
    attest False = throwError False
    attest True  = pure ()

    -- | Do a read.
    --
    -- This lifts a `readDuct` call up into a ThreadM, and compares
    -- the result it gets to an expected value.
    doRead :: forall a . Eq a => ReadDuct a -> Maybe a -> ThreadM ()
    doRead rd val = do
        val1 :: Maybe a <- withBlock $ readDuct rd
        attest $ (val1 == val)

    -- | Close a read duct.
    --
    -- This lifts a `closeReadDuct` up into a ThreadM, and compares
    -- the result it gets to an expected value.
    doCloseRead :: forall a . Eq a => ReadDuct a -> Maybe a -> ThreadM ()
    doCloseRead rd val = do
        val1 :: Maybe a <- liftIO $ closeReadDuct rd
        attest $ val1 == val

    -- | Do a write.
    --
    -- This lifts  a `writeDuct` up into a ThreadM, and compares
    -- the result it gets to an expected value.
    doWrite :: forall a . WriteDuct a -> a -> Open -> ThreadM ()
    doWrite wd val res = do
        r <- withBlock $ \onBlock -> writeDuct wd onBlock val
        attest $ r == res

    -- | Close a write duct.
    --
    -- This lifts a `closeWriteDuct` up into a ThreadM.  Note that becase
    -- closeWriteDuct does not return a value, no comparison of the result
    -- is done.
    doCloseWrite :: forall a . WriteDuct a -> ThreadM ()
    doCloseWrite wd = liftIO $ closeWriteDuct wd

    throwException :: forall a . Async a -> TestM ()
    throwException asy = liftIO $ cancelWith asy TestException

    expectException :: ThreadM () -> ThreadM ()
    expectException act =
        Ex.catch (act >> attest False) (\TestException -> pure ())

    -- If we create a full duct, we should be able to read the value from it
    -- and then close it without there being a new value.
    testReadFull :: Test
    testReadFull = runSingleThread "testReadFull" $ do
        let v :: Int
            v = 1
        (rd, _) <- liftIO $ newFullDuct v
        doRead rd (Just v)
        doCloseRead rd Nothing

    -- If we create a pre-closed duct, we should be able to close it.
    testReadClosed1 :: Test
    testReadClosed1 = runSingleThread "testReadClosed1" $ do
        (rd, _) <- liftIO $ newClosedDuct
        doCloseRead rd (Nothing :: Maybe Int)

    -- If we create a pre-closed duct, we should get a Nothing when we
    -- read from it.
    testReadClosed2 :: Test
    testReadClosed2 = runSingleThread "testReadClosed2" $ do
        (rd, _) <- liftIO $ newClosedDuct
        doRead rd (Nothing :: Maybe Int)
        doCloseRead rd (Nothing :: Maybe Int)

    -- If we create a full duct and close it, we should get a Nothing
    -- when we read from it.
    testReadClosed3 :: Test
    testReadClosed3 = runSingleThread "testReadClosed3" $ do
        (rd, _) <- liftIO $ newFullDuct (1 :: Int)
        doCloseRead rd (Just 1)
        doRead rd Nothing

    -- If we create an empty duct and close it, we should get a Nothing
    -- when we read from it.
    testReadClosed4 :: Test
    testReadClosed4 = runSingleThread "testReadClosed4" $ do
        (rd, _) <- liftIO $ newDuct
        doCloseRead rd (Nothing :: Maybe Int)
        doRead rd Nothing

    -- If we create a full duct, we should be able to close it and get
    -- the value.
    testCloseRead :: Test
    testCloseRead = runSingleThread "testCloseRead1" $ do
        let v :: Int
            v = 1
        (rd, _) <- liftIO $ newFullDuct v
        doCloseRead rd (Just v)

    -- If we create an empty duct, we should be able to write to it once.
    testWriteEmpty :: Test
    testWriteEmpty = runSingleThread "testWriteEmpty" $ do
        let v :: Int
            v = 1
        (_, wd) <- liftIO $ newDuct
        doWrite wd v Open
        doCloseWrite wd

    -- If we create a closed duct, a write should fail.
    testWriteClosed1 :: Test
    testWriteClosed1 = runSingleThread "testWriteClosed1" $ do
        let v :: Int
            v = 1
        (_, wd) <- liftIO $ newClosedDuct
        doWrite wd v Closed
        doCloseWrite wd

    -- If we close a full duct and close the write side, then a write
    -- should fail.
    testWriteClosed2 :: Test
    testWriteClosed2 = runSingleThread "testWriteClosed2" $ do
        (_, wd) <- liftIO $ newFullDuct (1 :: Int)
        doCloseWrite wd
        doWrite wd 1 Closed

    -- If we create an empty duct and close the write side, then a
    -- write should fail.
    testWriteClosed3 :: Test
    testWriteClosed3 = runSingleThread "testWriteClosed3" $ do
        (_, wd) <- liftIO $ newDuct
        doCloseWrite wd
        doWrite wd (1 :: Int) Closed

    testReadBlocks :: Test
    testReadBlocks = runTestM "testReadBlocks" $ do
            (rd, wd) <- liftIO $ newDuct
            _ <- waitForBlock $ spawn $ readSide rd
            _ <- noWaitForBlock $ spawn $ writeSide wd
            pure ()
        where
            readSide :: ReadDuct Int -> ThreadM ()
            readSide rd = do
                doRead rd (Just 1)
                doCloseRead rd Nothing

            writeSide :: WriteDuct Int -> ThreadM ()
            writeSide wd = do
                doWrite wd 1 Open
                doCloseWrite wd

    testReadQueues :: Test
    testReadQueues = runTestM "testReadQueues" $ do
            (rd, wd) <- lift $ newDuct
            _ <- waitForBlock $ spawn $ readSide rd 1
            _ <- waitForBlock $ spawn $ readSide rd 2
            _ <- waitForBlock $ spawn $ readSide rd 3
            _ <- noWaitForBlock $ spawn $ writeSide wd
            pure ()
        where
            readSide :: ReadDuct Int -> Int -> ThreadM ()
            readSide rd v = doRead rd (Just v)

            writeSide :: WriteDuct Int -> ThreadM ()
            writeSide wd = do
                doWrite wd 1 Open
                doWrite wd 2 Open
                doWrite wd 3 Open

    testWriteBlocks :: Test
    testWriteBlocks = runTestM "testWriteBlocks" $ do
            (rd, wd) <- lift $ newFullDuct (1 :: Int)
            _ <- waitForBlock $ spawn $ writeSide wd
            _ <- noWaitForBlock $ spawn $ readSide rd
            pure ()

        where
            readSide :: ReadDuct Int -> ThreadM ()
            readSide rd = do
                doRead rd (Just 1)
                doRead rd (Just 2)
                doCloseRead rd Nothing

            writeSide :: WriteDuct Int -> ThreadM ()
            writeSide wd = do
                doWrite wd 2 Open
                doCloseWrite wd


    testWriteQueues :: Test
    testWriteQueues = runTestM "testWriteQueues" $ do
            (rd, wd) <- lift $ newFullDuct (1 :: Int)
            _ <- waitForBlock $ spawn $ writeSide wd 2
            _ <- waitForBlock $ spawn $ writeSide wd 3
            _ <- waitForBlock $ spawn $ writeSide wd 4
            _ <- noWaitForBlock $ spawn $ readSide rd
            pure ()

        where
            readSide :: ReadDuct Int -> ThreadM ()
            readSide rd = do
                doRead rd (Just 1)
                doRead rd (Just 2)
                doRead rd (Just 3)
                doRead rd (Just 4)
                doCloseRead rd Nothing

            writeSide :: WriteDuct Int -> Int -> ThreadM ()
            writeSide wd x = do
                doWrite wd x Open


    testAbandonRead :: Test
    testAbandonRead = runTestM "testAbandonRead" $ do
            (rd, wd) <- lift $ newDuct
            a1 <- waitForBlock $ spawn $ exceptSide rd 1
            a2 <- waitForBlock $ spawn $ exceptSide rd 2
            _ <- waitForBlock $ spawn $ readSide rd 3
            throwException a1
            throwException a2
            _ <- noWaitForBlock $ spawn $ writeSide wd
            pure ()
        where
            exceptSide :: ReadDuct Int -> Int -> ThreadM ()
            exceptSide rd v =
                expectException $ doRead rd (Just v)

            readSide :: ReadDuct Int -> Int -> ThreadM ()
            readSide rd v = doRead rd (Just v)

            writeSide :: WriteDuct Int -> ThreadM ()
            writeSide wd = do
                doWrite wd 3 Open

    testAbandonWrite :: Test
    testAbandonWrite = runTestM "testAbandonWrite" $ do
            (rd, wd) <- lift $ newFullDuct (1 :: Int)
            a1 <- waitForBlock $ spawn $ exceptSide wd 2
            a2 <- waitForBlock $ spawn $ exceptSide wd 3
            _ <- waitForBlock $ spawn $ writeSide wd 4
            throwException a1
            throwException a2
            _ <- noWaitForBlock $ spawn $ readSide rd
            pure ()

        where
            readSide :: ReadDuct Int -> ThreadM ()
            readSide rd = do
                doRead rd (Just 1)
                doRead rd (Just 4)
                doCloseRead rd Nothing

            exceptSide :: WriteDuct Int -> Int -> ThreadM ()
            exceptSide wd x = expectException $ doWrite wd x Open

            writeSide :: WriteDuct Int -> Int -> ThreadM ()
            writeSide wd x = doWrite wd x Open

    testCloseReads1 :: Test
    testCloseReads1 = runTestM "testCloseReads1" $ do
            (rd, wd) <- lift $ newDuct
            _ <- waitForBlock $ spawn $ readSide rd
            _ <- waitForBlock $ spawn $ readSide rd
            _ <- noWaitForBlock $ spawn $ closeSide wd
            pure ()
        where
            readSide :: ReadDuct Int -> ThreadM ()
            readSide rd = doRead rd Nothing

            closeSide :: WriteDuct Int -> ThreadM ()
            closeSide wd = doCloseWrite wd 

    testCloseReads2 :: Test
    testCloseReads2 = runTestM "testCloseReads2" $ do
            (rd, _) <- lift $ newDuct
            _ <- waitForBlock $ spawn $ readSide rd
            _ <- waitForBlock $ spawn $ readSide rd
            _ <- noWaitForBlock $ spawn $ closeSide rd
            pure ()
        where
            readSide :: ReadDuct Int -> ThreadM ()
            readSide rd = doRead rd Nothing

            closeSide :: ReadDuct Int -> ThreadM ()
            closeSide rd = doCloseRead rd Nothing

    testCloseWrites1 :: Test
    testCloseWrites1 = runTestM "testCloseWrites1" $ do
            (rd, wd) <- lift $ newFullDuct (1 :: Int)
            _ <- waitForBlock $ spawn $ writeSide wd 2
            _ <- waitForBlock $ spawn $ writeSide wd 3
            _ <- noWaitForBlock $ spawn $ closeSide rd
            pure ()
        where
            writeSide :: WriteDuct Int -> Int -> ThreadM ()
            writeSide wd x = doWrite wd x Closed

            closeSide :: ReadDuct Int -> ThreadM ()
            closeSide rd = doCloseRead rd (Just 1)

    testCloseWrites2 :: Test
    testCloseWrites2 = runTestM "testCloseWrites2" $ do
            (_, wd) <- lift $ newFullDuct (1 :: Int)
            _ <- waitForBlock $ spawn $ writeSide wd 2
            _ <- waitForBlock $ spawn $ writeSide wd 3
            _ <- noWaitForBlock $ spawn $ closeSide wd
            pure ()
        where
            writeSide :: WriteDuct Int -> Int -> ThreadM ()
            writeSide wd x = doWrite wd x Closed

            closeSide :: WriteDuct Int -> ThreadM ()
            closeSide wd = doCloseWrite wd

