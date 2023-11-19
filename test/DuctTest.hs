{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DuctTest(
    tests
) where

    import           Control.Concurrent.Async
    import qualified Control.Concurrent.MVar             as MVar
    import           Control.Monad.Cont
    import           Control.Monad.Except
    import           Control.Monad.State
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.IORef
    import           Test.HUnit

    -- import qualified Control.Exception                   as Ex
    -- import           Data.Proxy                          (Proxy (..))

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
                    testReadBlocks -- ,
                    -- testReadQueues,
                    -- testWriteBlocks
                    -- testRead1,
                    -- testWrite1,
                    -- testClosed1,
                    -- testClosed2,
                    -- testReadBlock,
                    -- testWriteBlock,
                    -- testReadN,
                    -- testWriteN,
                    -- testAbandonRead,
                    -- testAbandonWrite,
                    -- testPreAbandonRead,
                    -- testPreAbandonWrite,
                    -- testClosedClose,
                    -- testCloseBlocked
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

    doRead :: forall a . Eq a => ReadDuct a -> Maybe a -> ThreadM ()
    doRead rd val = do
        val1 :: Maybe a <- withBlock $ readDuct rd
        attest $ (val1 == val)

    doCloseRead :: forall a . Eq a => ReadDuct a -> Maybe a -> ThreadM ()
    doCloseRead rd val = do
        val1 :: Maybe a <- liftIO $ closeReadDuct rd
        attest $ val1 == val

    doWrite :: forall a . WriteDuct a -> a -> Open -> ThreadM ()
    doWrite wd val res = do
        r <- withBlock $ \onBlock -> writeDuct wd onBlock val
        attest $ r == res

    doCloseWrite :: forall a . WriteDuct a -> ThreadM ()
    doCloseWrite wd = liftIO $ closeWriteDuct wd

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

    testWriteClosed1 :: Test
    testWriteClosed1 = runSingleThread "testWriteClosed1" $ do
        let v :: Int
            v = 1
        (_, wd) <- liftIO $ newClosedDuct
        doWrite wd v Closed
        doCloseWrite wd

    testWriteClosed2 :: Test
    testWriteClosed2 = runSingleThread "testWriteClosed2" $ do
        (_, wd) <- liftIO $ newFullDuct (1 :: Int)
        doCloseWrite wd
        doWrite wd 1 Closed

    testWriteClosed3 :: Test
    testWriteClosed3 = runSingleThread "testWriteClosed3" $ do
        (_, wd) <- liftIO $ newDuct
        doCloseWrite wd
        doWrite wd (1 :: Int) Closed

    testReadBlocks :: Test
    testReadBlocks = runTestM "testReadBlocks" $ do
            (rd, wd) <- liftIO $ newDuct
            rasy <- waitForBlock $ spawn $ readSide rd
            wasy <- noWaitForBlock $ spawn $ writeSide wd
            rres <- lift $ wait rasy
            wres <- lift $ wait wasy
            pure $ rres && wres
        where
            readSide :: ReadDuct Int -> ThreadM ()
            readSide rd = do
                doRead rd (Just 1)
                doCloseRead rd Nothing

            writeSide :: WriteDuct Int -> ThreadM ()
            writeSide wd = do
                doWrite wd 1 Open
                doCloseWrite wd

    {-
    testReadQueues :: Test
    testReadQueues = runTest "testReadQueues" $ do
            (rd, wd) <- lift $ newDuct
            rasy1 <- spawn $ readSide rd 1
            pause
            rasy2 <- spawn $ readSide rd 2
            pause
            rasy3 <- spawn $ readSide rd 3
            pause
            wasy <- spawn $ writeSide wd
            pause
            rres1 <- lift $ wait rasy1
            rres2 <- lift $ wait rasy2
            rres3 <- lift $ wait rasy3
            wres <- lift $ wait wasy
            pure $ rres1 && rres2 && rres3 && wres
        where
            readSide :: ReadDuct Int -> Int -> IO Bool
            readSide rd v = doRead rd (Just v)

            writeSide :: WriteDuct Int -> IO Bool
            writeSide wd =
                doWrite wd 1 Open
                `andThen` doWrite wd 2 Open
                `andThen` doWrite wd 3 Open


    testWriteBlocks :: Test
    testWriteBlocks = runTest " testWriteBlocks" $ do
            (rd, wd) <- lift $ newDuct
            wasy <- spawn $ writeSide wd
            pause
            rasy <- spawn $ readSide rd
            wres <- lift $ wait wasy
            rres <- lift $ wait rasy
            pure $ rres && wres
        where
            readSide :: ReadDuct Int -> IO Bool
            readSide rd = doRead rd (Just 1)
                            `andThen` doRead rd (Just 2)
                            `andThen` doCloseRead rd Nothing

            writeSide :: WriteDuct Int -> IO Bool
            writeSide wd = doWrite wd 1 Open
                        `andThen` doWrite wd 2 Open
                        `andThen` doCloseWrite wd




    data TestException = TestException deriving (Show)

    instance Ex.Exception TestException where

    spawnTest :: IO Bool -> M (Async Bool)
    spawnTest act = Codensity go
        where
            go :: forall b . (Async Bool -> IO b) -> IO b
            go cont = do
                withAsync act $ \asy -> do
                    link asy
                    b <- cont asy
                    r <- wait asy
                    assert r
                    pure b

    pause :: M ()
    pause = liftIO $ threadDelay 500

    runTestBase :: M () -> Test
    runTestBase act = TestCase $ runCodensity act pure

    runTest :: String -> M () -> Test
    runTest label = TestLabel label . runTestBase

    testWrite :: WriteDuct Int -> Int -> Open -> IO Bool
    testWrite wd val op = do
        op2 :: Open <- writeDuct wd val
        pure $ op == op2

    testExWith :: forall e . Ex.Exception e => Proxy e -> IO Bool -> IO Bool
    testExWith Proxy act = (act >> pure False) `Ex.catch` handler
        where
            handler :: e -> IO Bool
            handler _ = pure True

    testEx :: IO Bool -> IO Bool
    testEx = testExWith (Proxy :: Proxy TestException)

    cancelTest :: Async Bool -> M ()
    cancelTest asy = liftIO $ cancelWith asy TestException

    testRead1 :: Test
    testRead1 = runTest "testRead1" $ do
                    -- A read on a full duct always succeeds
                    (rd, _) <- liftIO $ newFullDuct 1
                    _ <- spawnTest $ testRead rd (Just 1)
                    pure ()

    testWrite1 :: Test
    testWrite1 = runTest "testWrite1" $ do
                    -- A write on an empty duct always succeeds
                    (_, wd) <- liftIO $ newDuct
                    _ <- spawnTest $ testWrite wd 1 Open
                    pure ()

    testClosed1 :: Test
    testClosed1 = runTest "testClosed1" $ do
                    -- A read on a closed duct always fails.
                    (rd, _) <- liftIO $ newClosedDuct
                    _ <- spawnTest $ testRead rd Nothing
                    pure ()

    testClosed2 :: Test
    testClosed2 = runTest "testClosed1" $ do
                    -- A write on a closed duct always fails.
                    (_, wd) <- liftIO $ newClosedDuct
                    _ <- spawnTest $ testWrite wd 1 Closed
                    pure ()

    testReadBlock :: Test
    testReadBlock = runTest "testReadBlock" $ do
                        -- A read on an empty duct blocks until a write
                        (rd, wd) <- liftIO $ newDuct
                        _ <- spawnTest $ testRead rd (Just 1)
                        pause
                        _ <- spawnTest $ testWrite wd 1 Open
                        pure ()

    testWriteBlock :: Test
    testWriteBlock = runTest "testWriteBlock" $ do
                        -- a write on a full duct blocks until a read
                        (rd, wd) <- liftIO $ newFullDuct 1
                        _ <- spawnTest $ testWrite wd 2 Open
                        pause
                        _ <- spawnTest $ testRead rd (Just 1)
                        pause
                        _ <- spawnTest $ testRead rd (Just 2)
                        pure ()

    testReadN :: Test
    testReadN = TestLabel "testReadN" $ TestList $ makeTest <$> [ 2 .. 7 ]
        where
            -- We cue up N reads, then do N writes, and make sure everything
            -- happens in order.
            makeTest :: Int -> Test
            makeTest n = runTest ("testRead" ++ show n) $ do
                (rd, wd) <- liftIO $ newDuct
                mapM_ (spawnRead rd) [ 1 .. n ]
                mapM_ (spawnWrite wd) [ 1 .. n ]

            spawnRead :: ReadDuct Int -> Int -> M ()
            spawnRead rd i = do
                _ <- spawnTest $ testRead rd (Just i)
                pause

            spawnWrite :: WriteDuct Int -> Int -> M ()
            spawnWrite wd i = do
                _ <- spawnTest $ testWrite wd i Open
                pause

    testWriteN :: Test
    testWriteN = TestLabel "testWriteN" $ TestList $ makeTest <$> [ 2 .. 7 ]
        where
            -- We cue up N writes, then do N reads and make sure everything
            -- happens in the correct order.
            makeTest :: Int -> Test
            makeTest n = runTest ("testWrite" ++ show n) $ do
                (rd, wd) <- liftIO $ newFullDuct 1
                mapM_ (spawnWrite wd) [ 2 .. (n+1) ]
                mapM_ (spawnRead rd) [ 1 .. (n+1) ]

            spawnRead :: ReadDuct Int -> Int -> M ()
            spawnRead rd i = do
                _ <- spawnTest $ testRead rd (Just i)
                pause

            spawnWrite :: WriteDuct Int -> Int -> M ()
            spawnWrite wd i = do
                _ <- spawnTest $ testWrite wd i Open
                pause

    testAbandonRead :: Test
    testAbandonRead = runTest "testAbandonRead" $ do
        -- We spawn two reads, then cause the first read to abort by
        -- throwing an exception.  A single write then should satisfy
        -- the second read.
        (rd, wd) <- liftIO $ newDuct
        asy1 <- spawnTest $ testEx $ testRead rd (Just 0)
        pause
        _ <- spawnTest $ testRead rd (Just 1)
        pause
        cancelTest asy1
        pause
        _ <- spawnTest $ testWrite wd 1 Open
        pure ()

    testAbandonWrite :: Test
    testAbandonWrite = runTest "testAbandonWrite" $ do
        -- Like testAbandonRead, we spawn two writes, then force the
        -- first write to throw an exception.  A read should then
        -- be satisified by the second write.
        (rd, wd) <- liftIO $ newFullDuct 1
        asy1 <- spawnTest $ testEx $ testWrite wd 2 Open
        pause
        _ <- spawnTest $ testWrite wd 3 Open
        pause
        cancelTest asy1
        -- We need to read the value the duct was created with
        _ <- spawnTest $ testRead rd (Just 1)
        pause
        -- And then we see the second value written.
        _ <- spawnTest $ testRead rd (Just 3)
        pure ()


    testPreAbandonRead :: Test
    testPreAbandonRead = runTest "testAbandonRead" $ do
        -- Like testAbandonRead, except we abandon the first read before
        -- spawning the second.
        (rd, wd) <- liftIO $ newDuct
        asy1 <- spawnTest $ testEx $ testRead rd (Just 0)
        pause
        cancelTest asy1
        pause
        _ <- spawnTest $ testRead rd (Just 1)
        pause
        _ <- spawnTest $ testWrite wd 1 Open
        pure ()

    testPreAbandonWrite :: Test
    testPreAbandonWrite = runTest "testAbandonWrite" $ do
        -- Like testAbandonWrite, but like testPreAbandonRead we abandon
        -- the first write before spawning the second.
        (rd, wd) <- liftIO $ newFullDuct 1
        asy1 <- spawnTest $ testEx $ testWrite wd 2 Open
        pause
        cancelTest asy1
        pause
        _ <- spawnTest $ testWrite wd 3 Open
        pause
        -- We need to read the value the duct was created with
        _ <- spawnTest $ testRead rd (Just 1)
        pause
        -- And then we see the second value written.
        _ <- spawnTest $ testRead rd (Just 3)
        pure ()

    type Ducts = (ReadDuct Int, WriteDuct Int)

    testClosedClose :: Test
    testClosedClose = TestLabel "closedClose" $
                    TestList [ createClosed, createEmpty, createFull ]
        where
            createClosed :: Test
            createClosed = TestLabel "Duct initially closed" $
                                pickClosed (doTest Nothing newClosedDuct) False

            createEmpty :: Test
            createEmpty = TestLabel "Duct initially empty" $
                            pickClosed (doTest Nothing newDuct) True

            createFull :: Test
            createFull = TestLabel "Duct initially full" $
                            pickClosed (doTest (Just 1) (newFullDuct 1)) True

            pickClosed ::
                (Maybe ((Ducts -> M (Maybe Int)),
                            Maybe (Ducts -> M (Maybe Int)))
                    -> Test)
                -> Bool
                -> Test
            pickClosed cont needAClose =
                    TestList $ go <$> (if needAClose
                                        then closeOpts
                                        else
                                            -- Add the "no closes" option
                                            -- to the list.
                                            (("No closes", Nothing)
                                                : closeOpts))
                where
                    go :: (String, Maybe ((Ducts -> M (Maybe Int)),
                                        Maybe (Ducts -> M (Maybe Int))))
                            -> Test
                    go (lbl, closes) = TestLabel lbl $ cont closes


            closeOpts :: [ (String,
                                Maybe ((Ducts -> M (Maybe Int)),
                                        Maybe (Ducts -> M (Maybe Int)))) ]
            closeOpts = [
                ("Only Read", Just (readClose, Nothing)),
                ("Only Write", Just (writeClose, Nothing)),
                ("Double read", Just (readClose, Just readClose)),
                ("Read then write", Just (readClose, Just writeClose)),
                ("Write then read", Just (writeClose, Just readClose)),
                ("Double write", Just (writeClose, Just writeClose)) ]

            readClose :: Ducts -> M (Maybe Int)
            readClose (rd, _) = liftIO $ closeReadDuct rd

            writeClose :: Ducts -> M (Maybe Int)
            writeClose (_, wd) = liftIO $ closeWriteDuct wd

            doTest ::
                Maybe Int
                -> IO Ducts
                -> Maybe ((Ducts -> M (Maybe Int)),
                            Maybe (Ducts -> M (Maybe Int)))
                -> Test
            doTest expected createDucts closeOp = runTestBase $ do
                ducts <- liftIO $ createDucts
                case closeOp of
                    Nothing -> pure ()
                    Just (op, sec) -> do
                        res <- op ducts
                        liftIO $ assert (res == expected)
                        case sec of
                            Nothing -> pure ()
                            Just op2 -> do
                                res2 <- op2 ducts
                                -- From the second close, we always expect
                                -- Nothing.
                                liftIO $ assert (res2 == Nothing)

                _ <- spawnTest $ testRead (fst ducts) Nothing
                pause
                _ <- spawnTest $ testWrite (snd ducts) 1 Closed
                pause
                pure ()

    testCloseBlocked :: Test
    testCloseBlocked = TestLabel "testCloseBlocked" $ TestList [ rtest, wtest ]
        where
            rtest :: Test
            rtest = TestLabel "reads blocking" $
                        pickClose (doTest Nothing makeDucts blockOp
                                        readOp writeOp)
                where
                    makeDucts :: M Ducts
                    makeDucts = liftIO $ newDuct

                    blockOp :: Ducts -> M ()
                    blockOp (rd, _) = do
                        _ <- spawnTest $ testRead rd Nothing
                        -- We don't pause here because ordering isn't
                        -- important.
                        pure ()

            readOp :: Ducts -> M ()
            readOp (rd, _) = do
                _ <- spawnTest $ testRead rd (Just 1)
                pause
                pure ()

            writeOp :: Ducts -> M ()
            writeOp (_, wd) = do
                _ <- spawnTest $ testWrite wd 1 Open
                pause
                pure ()

            wtest :: Test
            wtest = TestLabel "writes blocking" $
                        pickClose (doTest (Just 1) makeDucts blockOp
                                        writeOp readOp)
                where
                    makeDucts :: M Ducts
                    makeDucts = liftIO $ newFullDuct 1

                    blockOp :: Ducts -> M ()
                    blockOp (_, wd) = do
                        _ <- spawnTest $ testWrite wd 1 Closed
                        -- We don't pause here because ordering isn't
                        -- important.
                        pure ()

            pickClose ::
                ((Ducts -> M (Maybe Int)) -> Int -> Int -> Test)
                -> Test
            pickClose cont = TestList [ closeRead, closeWrite ]
                where
                    closeRead :: Test
                    closeRead = pickNumBlocks $ cont $
                                    \(rd, _) -> liftIO $ closeReadDuct rd

                    closeWrite :: Test
                    closeWrite = pickNumBlocks $ cont $
                                    \(_, wd) -> liftIO $ closeWriteDuct wd


            pickNumBlocks ::
                (Int -> Int -> Test)
                -> Test
            pickNumBlocks cont = TestList $ go <$> [ 1 .. 5 ]
                where
                    go :: Int -> Test
                    go num = TestLabel ("With " ++ show num ++ " blocks") $
                                pickNumPreops $ cont num

            pickNumPreops ::
                (Int -> Test)
                -> Test
            pickNumPreops cont = TestList $ go <$> [ 0 .. 3 ]
                where
                    go :: Int -> Test
                    go num = TestLabel ("With " ++ show num ++ " preops") $
                                cont num

            doTest ::
                Maybe Int
                -> M Ducts
                -> (Ducts -> M ())
                -> (Ducts -> M ())
                -> (Ducts -> M ())
                -> (Ducts -> M (Maybe Int))
                -> Int
                -> Int
                -> Test
            doTest expected makeDucts blockOp preOp postOp closeOp
                    numBlocks numPreOps =
                runTestBase $ do
                    ducts <- makeDucts
                    mapM_ (\_ -> preOp ducts) [ 1 .. numPreOps ]
                    mapM_ (\_ -> blockOp ducts) [ 1 .. numBlocks ]
                    pause
                    mapM_ (\_ -> postOp ducts) [ 1 .. numPreOps ]
                    res <- closeOp ducts
                    liftIO $ assert (res == expected)

    -}
