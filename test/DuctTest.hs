{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DuctTest(
    tests
) where

    import           Control.Monad.Cont
    import           Data.Conduit.Parallel.Internal.Duct
    import           DuctInfra
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
                    testWriteBlocks2,
                    testReadQueues
                ]

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

    -- | If we create a full duct, writes block until a read happens.
    testWriteBlocks2 :: Test
    testWriteBlocks2 = runTestM "testWriteBlocks" $ do
            (src, snk) <- liftIO $ newFullDuct (1 :: Int)
            liftIO $ addReadOpens src 1
            _ <- waitForBlock $ spawn $ writeSide snk
            _ <- noWaitForBlock $ spawn $ readSide src
            pure ()
        where
            readSide :: ReadDuct Int -> ThreadM ()
            readSide src =
                withRead src $ \rd -> do
                    doRead rd (Just 1)
                    doRead rd (Just 2)

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
