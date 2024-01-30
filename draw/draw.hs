#!/usr/bin/env stack
-- stack script --resolver lts-21.20 --package blaze-svg,blaze-markup,filepath,directory --ghc-options -W --ghc-options -Wall --ghc-options -Werror

{-# LANGUAGE OverloadedStrings #-}

import           Circuit
import           Fuse
import           Misc
import           Simple
import           System.Directory               (doesDirectoryExist)
import           System.Environment             (getArgs)
import           System.Exit                    (exitFailure)
import           System.FilePath                ((</>))
import           Tee
import qualified Text.Blaze.Svg.Renderer.String as Blaze
import           Text.Blaze.Svg11               (Svg, (!))
import qualified Text.Blaze.Svg11               as S
import qualified Text.Blaze.Svg11.Attributes    as A

writeSVG :: FilePath -> Svg -> IO ()
writeSVG fname svg = do
    putStrLn $ "Writing file " ++ show fname
    writeFile fname $
        Blaze.renderSvg $
            S.docTypeSvg
                ! A.version "1.1"
                ! A.width "320"
                ! A.height "240"
                ! A.viewbox "0 0 640 480"
                $ S.g   ! A.fill "none"
                        ! A.stroke "black"
                        ! A.color "black"
                        ! A.fontSize "32"
                        ! A.dominantBaseline "middle"
                        ! A.textAnchor "middle"
                        ! A.fontFamily "sans-serif"
                    $ svg

getOutputDir :: IO FilePath
getOutputDir = do
        args <- getArgs
        case args of
            [ "-o", odir ] -> do
                b <- doesDirectoryExist odir
                if b
                then pure odir
                else printUsage
            _              -> printUsage
    where

        printUsage :: IO FilePath
        printUsage = do
            putStrLn "Usage: draw.hs -o <odir>"
            putStrLn ""
            putStrLn "Where <odir> is the directory (relative to the current"
            putStrLn "working directory) to store the created SVG files."
            putStrLn ""
            exitFailure

main :: IO ()
main = do
    odir <- getOutputDir
    writeSVG (odir </> "simple.svg") simple
    writeSVG (odir </> "fuse.svg") fuse
    writeSVG (odir </> "fuseLeft.svg") fuseLeft
    writeSVG (odir </> "fuseMap.svg") fuseMap
    writeSVG (odir </> "fuseSemigroup.svg") fuseSemigroup
    writeSVG (odir </> "fuseTuple.svg") fuseTuple
    writeSVG (odir </> "proFunctor.svg") proFunctor
    writeSVG (odir </> "parallel.svg") parallel
    writeSVG (odir </> "tee.svg") tee
    writeSVG (odir </> "merge.svg") merge
    writeSVG (odir </> "routeEither.svg") routeEither
    writeSVG (odir </> "routeThese.svg") routeThese
    writeSVG (odir </> "routeTuple.svg") routeTuple
    writeSVG (odir </> "fixP.svg") fixP
    putStrLn "Done."
