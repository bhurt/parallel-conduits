{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- Infrastruct for the Duct tests.
module DuctInfra where

    import           Control.Concurrent.Async
    import qualified Control.Concurrent.MVar             as MVar
    import qualified Control.Exception.Lifted            as Ex
    import           Control.Monad.Cont
    import           Control.Monad.Reader
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.IORef
    import           Test.HUnit

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


