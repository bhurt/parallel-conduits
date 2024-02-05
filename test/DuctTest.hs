{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DuctTest(
    tests
) where

    import           Control.Concurrent.Async
    import qualified Control.Concurrent.MVar             as MVar
    import qualified Control.Exception.Lifted            as Ex
    import           Control.Monad.Cont
    import           Control.Monad.Reader
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.IORef
    import           Test.HUnit

    tests :: Test
    tests = TestLabel "Duct Tests" $
                TestList [
                    testReadFull,
                    testWriteEmpty,
                    testReadClosed,
                    testWriteClosed,
                    testReadClosing1,
                    testWriteClosing1,
                    testReadClosing2,
                    testWriteClosing2,
                    testReadBlocks,
                    testWriteBlocks,
                    testReadQueues
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
    type ThreadM = ReaderT (Maybe (IO ())) IO

    -- | A known exception we can throw at threads.
    data TestException = TestException deriving (Show)

    instance Ex.Exception TestException where

    data  TestAbort = TestAbort deriving (Show)
    instance Ex.Exception TestAbort

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
            let act1 :: IO ()
                act1 = runReaderT act Nothing

            mact :: Either TestAbort () <- Ex.try act1
            case mact of
                Left _   -> assert False
                Right () -> assert True

    -- | Wait for a block before continuing.
    --
    -- This function is intended to wrap `spawn`, causing the `TestM`
    -- code to wait for the newly spawned thread to block before
    -- continuing.
    waitForBlock :: (Maybe (IO ()) -> TestM a) -> TestM a
    waitForBlock go = do
            mvar <- liftIO $ MVar.newEmptyMVar
            ref <- liftIO $ newIORef (Just (MVar.putMVar mvar ()))
            a <- go (Just (runRef ref))
            _ <- liftIO $ MVar.takeMVar mvar
            pure a
        where
            runRef :: IORef (Maybe (IO ())) -> IO ()
            runRef ref = do
                mact :: Maybe (IO ()) <- readIORef ref
                case mact of
                    Nothing -> pure ()
                    Just act -> do
                        writeIORef ref Nothing
                        act

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
                let a1 :: IO ()
                    a1 = runReaderT a onBlck

                mact :: Either TestAbort () <- Ex.try a1

                -- Ensure the onBlock action has been performed.
                case onBlck of
                    Nothing   -> pure ()
                    Just blck -> blck

                pure $ case mact of
                            Left  _  -> False
                            Right () -> True


    -- | Lift an IO action taking a onBlock value to a `ThreadM`.
    --
    -- This is intended to be used by `doRead` and `doWrite`.  If
    -- the underlying IO action doesn't need or use the onBlock
    -- action, then just use liftIO.
    withBlock :: forall a . (Maybe (IO ()) -> ThreadM a) -> ThreadM a
    withBlock op = do
            s <- ask
            op s

    withRead :: forall a b .
                    ReadDuct a
                    -> (IO (Maybe a) -> ThreadM b)
                    -> ThreadM b
    withRead src act =
        withBlock $ \onBlock -> withReadDuct src onBlock act

    withWrite :: forall a b .
                    WriteDuct a
                    -> ((a -> IO Open) -> ThreadM b)
                    -> ThreadM b
    withWrite snk act =
        withBlock $ \onBlock -> withWriteDuct snk onBlock act

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
    attest False = Ex.throw TestAbort
    attest True  = pure ()

    -- | Do a read.
    --
    -- This lifts a `readDuct` call up into a ThreadM, and compares
    -- the result it gets to an expected value.
    doRead :: forall a . (Show a, Eq a) => IO (Maybe a) -> Maybe a -> ThreadM ()
    doRead rd val = do
        val1 :: Maybe a <- liftIO rd
        if (val1 /= val)
        then
            liftIO . putStrLn $  "doRead failed: expected " ++ show val
                                    ++ " but got " ++ show val1
        else pure ()
        attest $ (val1 == val)

    -- | Do a write.
    --
    -- This lifts  a `writeDuct` up into a ThreadM, and compares
    -- the result it gets to an expected value.
    doWrite :: forall a . Show a => (a -> IO Open) -> a -> Open -> ThreadM ()
    doWrite wd val res = do
        r <- liftIO $ wd val
        if (r /= res)
        then
            liftIO . putStrLn $ "doWrite failed: expected "
                                    ++ show res
                                    ++ " but got "
                                    ++ show r
                                    ++ " writing " ++ show val
        else pure ()
        attest $ r == res

    {-
    throwException :: forall a . Async a -> TestM ()
    throwException asy = liftIO $ cancelWith asy TestException

    expectException :: ThreadM () -> ThreadM ()
    expectException act =
        Ex.catch (act >> attest False) (\TestException -> pure ())
    -}

    -- If we create a full duct, we should be able to read the value from it
    -- and then close it without there being a new value.
    testReadFull :: Test
    testReadFull = runSingleThread "testReadFull" $ do
        let v :: Int
            v = 1
        (src, _) <- liftIO $ newFullDuct v
        withRead src $ \rd -> doRead rd (Just v)

    -- If we create an empty duct, we should be able to write to it once.
    testWriteEmpty :: Test
    testWriteEmpty = runSingleThread "testWriteEmpty" $ do
        (_, snk) <- liftIO $ newDuct
        withWrite snk $ \wd -> doWrite wd (1 :: Int) Open

    -- If we create a pre-closed duct, we should get a Nothing when we
    -- read from it.
    testReadClosed :: Test
    testReadClosed = runSingleThread "testReadClosed" $ do
        let src :: ReadDuct Int
            (src, _) = newClosedDuct
        withRead src $ \rd -> doRead rd (Nothing :: Maybe Int)

    -- If we create a pre-closed duct, we should get Closed when we
    -- try to write to it.
    testWriteClosed :: Test
    testWriteClosed = runSingleThread "testWriteClosed" $ do
        let snk :: WriteDuct Int
            (_, snk) = newClosedDuct
        withWrite snk $ \wd -> doWrite wd (1 :: Int) Closed

    -- If we create an empty duct, and close the write side, we should
    -- get Nothing when we try to read from it.
    testReadClosing1 :: Test
    testReadClosing1 = runSingleThread "testReadClosing1" $ do
        (src, snk) <- liftIO $ newDuct
        withWrite snk $ \_ -> pure ()
        withRead src $ \rd -> doRead rd (Nothing :: Maybe Int)

    -- If we create an empty duct, and close the read side, we should
    -- not be able to write to it.
    testWriteClosing1 :: Test
    testWriteClosing1 = runSingleThread "testWriteClosing1" $ do
        (src, snk) <- liftIO $ newDuct
        withRead src $ \_ -> pure ()
        withWrite snk $ \wd -> doWrite wd (1 :: Int) Closed

    -- | If we create a full duct, and close the write side, we should
    -- be able to read it once, but the second read should be Nothing.
    testReadClosing2 :: Test
    testReadClosing2 = runSingleThread "testReadClosing2" $ do
        (src, snk) <- liftIO $ newFullDuct (1 :: Int)
        withWrite snk $ \_ -> pure ()
        withRead src $ \rd -> do
            doRead rd (Just 1)
            doRead rd Nothing

    -- | If we create a full duct, and close the read side, we should
    -- still not be able to write to it.
    testWriteClosing2 :: Test
    testWriteClosing2 = runSingleThread "testWriteClosing2" $ do
        (src, snk) <- liftIO $ newFullDuct (1 :: Int)
        withRead src $ \_ -> pure ()
        withWrite snk $ \wd -> doWrite wd 2 Closed


    -- | If we create an empty duct, a read blocks until a write happens.
    testReadBlocks :: Test
    testReadBlocks = runTestM "testReadBlocks" $ do
            (src, snk) <- liftIO $ newDuct
            _ <- waitForBlock $ spawn $ readSide src
            _ <- noWaitForBlock $ spawn $ writeSide snk
            pure ()
        where
            readSide :: ReadDuct Int -> ThreadM ()
            readSide src =
                withRead src $ \rd -> doRead rd (Just 1)

            writeSide :: WriteDuct Int -> ThreadM ()
            writeSide snk =
                withWrite snk $ \wd -> doWrite wd 1 Open

    -- | If we create a full duct, writes block until a read happens.
    testWriteBlocks :: Test
    testWriteBlocks = runTestM "testWriteBlocks" $ do
            (src, snk) <- liftIO $ newFullDuct (1 :: Int)
            liftIO $ addReadOpens src 1
            _ <- waitForBlock $ spawn $ writeSide snk
            _ <- noWaitForBlock $ spawn $ readSide src
            pure ()
        where
            readSide :: ReadDuct Int -> ThreadM ()
            readSide src =
                withRead src $ \rd -> doRead rd (Just 1)

            writeSide :: WriteDuct Int -> ThreadM ()
            writeSide snk =
                withWrite snk $ \wd -> doWrite wd 2 Open

    -- | If we pile up multiple reads on an empty queue, they all block
    -- until writes happen, and then they are satisified in order.
    testReadQueues :: Test
    testReadQueues = runTestM "testReadUqueues" $ do
            (src, snk) :: (ReadDuct Int, WriteDuct Int) <- liftIO $ newDuct
            liftIO $ addReadOpens src 2
            _ <- waitForBlock $ spawn $ readSide src 1
            _ <- waitForBlock $ spawn $ readSide src 2
            _ <- waitForBlock $ spawn $ readSide src 3
            _ <- noWaitForBlock $ spawn $ writeSide snk
            pure ()
        where
            readSide :: ReadDuct Int -> Int -> ThreadM ()
            readSide src v = withRead src $ \rd -> doRead rd (Just v)

            writeSide :: WriteDuct Int -> ThreadM ()
            writeSide snk = withWrite snk $ \wd -> do
                doWrite wd 1 Open
                doWrite wd 2 Open
                doWrite wd 3 Open


        

        









{-

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

-}
