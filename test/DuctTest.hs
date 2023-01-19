{-# LANGUAGE ScopedTypeVariables #-}

module DuctTest(
    tests
) where

    import           Control.Concurrent                  (threadDelay)
    import           Control.Concurrent.Async
    import qualified Control.Exception                   as Ex
    import           Control.Monad.Codensity
    import           Control.Monad.IO.Class
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Proxy                          (Proxy (..))
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
                    testReadN,
                    testWriteN,
                    testAbandonRead
                ]

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

    runTest :: String -> M () -> Test
    runTest label act = TestLabel label $ TestCase $ runCodensity act pure

    testRead :: ReadDuct Int -> Maybe Int -> IO Bool
    testRead rd val = do
        val1 :: Maybe a <- readDuct rd
        pure $ val1 == val

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
        (rd, wd) <- liftIO $ newDuct
        asy1 <- spawnTest $ testEx $ testRead rd (Just 0)
        pause
        _ <- spawnTest $ testRead rd (Just 1)
        pause
        cancelTest asy1
        _ <- spawnTest $ testWrite wd 1 Open
        pure ()



