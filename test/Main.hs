
module Main(
    main
) where

    import qualified DuctTest
    import           Test.HUnit

    allTests :: Test
    allTests = TestLabel "All Tests" $
                TestList [
                    DuctTest.tests
                ]

    main :: IO ()
    main = runTestTTAndExit allTests

