{-# LANGUAGE ScopedTypeVariables #-}

module DuctTest(
    tests
) where

    import           Control.Concurrent                  (threadDelay)
    import           Control.Concurrent.Async
    import           Control.Monad.Codensity
    import           Control.Monad.IO.Class
    import           Data.Conduit.Parallel.Internal.Duct
    import           Test.HUnit

    type M = Codensity IO

    tests :: Test
    tests = TestLabel "Duct Tests" $
                TestList [
                    testRead1,
                    testWrite1,
                    testClosed1,
                    testClosed2,
                    testReadBlock,
                    testWriteBlock,
                    testRead2,
                    testWrite2,
                    testRead3,
                    testWrite3
                ]

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

    runTest :: String -> M () -> Test
    runTest label act = TestLabel label $ TestCase $ runCodensity act pure

    testRead :: ReadDuct Int -> Maybe Int -> IO Bool
    testRead rd val = do
        val1 :: Maybe a <- readDuct rd
        if (val1 /= val)
        then putStrLn $ "Expected " ++ show val ++ " but got " ++ show val1
        else pure ()
        pure $ val1 == val

    testWrite :: WriteDuct Int -> Int -> Open -> IO Bool
    testWrite wd val op = do
        op2 :: Open <- writeDuct wd val
        pure $ op == op2

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

    testRead2 :: Test
    testRead2 = runTest "testRead2" $ do
                    -- Two reads get filled in order
                    (rd, wd) <- liftIO $ newDuct
                    _ <- spawnTest $ testRead rd (Just 1)
                    pause
                    _ <- spawnTest $ testRead rd (Just 2)
                    pause
                    _ <- spawnTest $ testWrite wd 1 Open
                    pause
                    _ <- spawnTest $ testWrite wd 2 Open
                    pure ()

    testWrite2 :: Test
    testWrite2 = runTest "testWrite2" $ do
                    -- Two writes get filled in order
                    (rd, wd) <- liftIO $ newFullDuct 1
                    _ <- spawnTest $ testWrite wd 2 Open
                    pause
                    _ <- spawnTest $ testWrite wd 3 Open
                    pause
                    _ <- spawnTest $ testRead rd (Just 1)
                    pause
                    _ <- spawnTest $ testRead rd (Just 2)
                    pause
                    _ <- spawnTest $ testRead rd (Just 3)
                    pure ()

    testRead3 :: Test
    testRead3 = runTest "testRead2" $ do
                    -- Three reads get filled in order
                    (rd, wd) <- liftIO $ newDuct
                    _ <- spawnTest $ testRead rd (Just 1)
                    pause
                    _ <- spawnTest $ testRead rd (Just 2)
                    pause
                    _ <- spawnTest $ testRead rd (Just 3)
                    pause
                    _ <- spawnTest $ testWrite wd 1 Open
                    pause
                    _ <- spawnTest $ testWrite wd 2 Open
                    pause
                    _ <- spawnTest $ testWrite wd 3 Open
                    pure ()

    testWrite3 :: Test
    testWrite3 = runTest "testWrite2" $ do
                    -- Three writes get filled in order
                    (rd, wd) <- liftIO $ newFullDuct 1
                    _ <- spawnTest $ testWrite wd 2 Open
                    pause
                    _ <- spawnTest $ testWrite wd 3 Open
                    pause
                    _ <- spawnTest $ testWrite wd 4 Open
                    pause
                    _ <- spawnTest $ testRead rd (Just 1)
                    pause
                    _ <- spawnTest $ testRead rd (Just 2)
                    pause
                    _ <- spawnTest $ testRead rd (Just 3)
                    pause
                    _ <- spawnTest $ testRead rd (Just 4)
                    pure ()

