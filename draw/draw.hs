#!/usr/bin/env stack
-- stack script --resolver lts-21.20 --package blaze-svg,blaze-markup,filepath,directory

{-# LANGUAGE OverloadedStrings #-}

{-# OPTIONS_GHC -W -Wall -Werror #-}

import           Data.Fixed                     (Deci)
import           Data.List                      (intercalate)
import           Data.String
import           System.Directory               (doesDirectoryExist)
import           System.Environment             (getArgs)
import           System.Exit                    (exitFailure)
import           System.FilePath                ((</>))
import           Text.Blaze.Internal            (MarkupM)
import qualified Text.Blaze.Svg.Renderer.String as Blaze
import           Text.Blaze.Svg11               (Svg, (!))
import qualified Text.Blaze.Svg11               as S
import qualified Text.Blaze.Svg11.Attributes    as A

data Point = Point {
                x :: Deci,
                y :: Deci }

-- This used to be:
--     data Direction = Up | Down | Left | Right
-- but Left and Right conflicted with Either's Left and Right.
data Direction = North | South | West | East
    deriving (Show, Read, Ord, Eq, Enum, Bounded)

move :: Direction -> Deci -> Point -> Point
move North step pt = pt { y = y pt - step }
move South step pt = pt { y = y pt + step }
move West  step pt = pt { x = x pt - step }
move East  step pt = pt { x = x pt + step }

-- rotate 90 degrees counter-clockwise.
rot :: Direction -> Direction
rot North = West
rot West  = South
rot South = East
rot East  = North

-- rotate 180 degrees
rev :: Direction -> Direction
rev North = South
rev West  = East
rev South = North
rev East  = West

data ControlPoints = ControlPoints {
                            top :: Point,
                            left :: Point,
                            bottom :: Point,
                            right :: Point }

deci :: (IsString s) => Deci -> s
deci = fromString . fixString . show
    where
        fixString :: String -> String
        fixString []     = []
        fixString ".0"   = ""
        fixString (x:xs) = x : fixString xs

renderPolygonPoints :: [ Point ] -> String
renderPolygonPoints pts = intercalate " " $ renderPoint <$> pts
    where
        renderPoint :: Point -> String
        renderPoint pt = deci (x pt) ++ "," ++ deci (y pt)



rect :: Point -> Deci -> MarkupM ControlPoints
rect org scale = controls <$ svg
    where
        xstep :: Deci
        xstep = scale * 2

        ystep :: Deci
        ystep = scale * 1.5

        controls :: ControlPoints
        controls = ControlPoints {
                    top = move North ystep org,
                    bottom = move South ystep org,
                    left = move West xstep org,
                    right = move East xstep org }

        topWest :: Point
        topWest = move North ystep $ move West xstep org

        topEast :: Point
        topEast = move North ystep $ move East xstep org

        bottomWest :: Point
        bottomWest = move South ystep $ move West xstep org

        bottomEast :: Point
        bottomEast = move South ystep $ move East xstep org

        points :: String
        points = renderPolygonPoints
                    [ topWest, topEast, bottomEast, bottomWest ]

        svg :: Svg
        svg = S.polygon
                ! A.points (fromString points)
                ! A.strokeWidth "4"


text :: Point -> String -> Svg
text org txt = S.text_
                    ! A.x (deci (x org))
                    ! A.y (deci (y org))
                    ! A.fill "black"
                    $ S.text (fromString txt)

arrowHead :: Direction -> Point -> Svg
arrowHead dir pt = S.polygon
                        ! A.points (fromString points)
                        ! A.strokeWidth "1"
                        ! A.fill "black"

    where
        pt2 :: Point
        pt2 = move (rev dir) 12 $ move (rot dir) 8 pt

        pt3 :: Point
        pt3 = move (rev dir) 10 pt

        pt4 :: Point
        pt4 = move (rev (rot dir)) 16 pt2

        points :: String
        points = renderPolygonPoints [ pt, pt2, pt3, pt4 ]

data Move = Move {
                dir :: Direction,
                dist :: Deci }

route :: Point -> [ Move ] -> Svg
route start moves =
        S.polyline 
            ! A.points (fromString points)
            ! A.strokeWidth "2"
        <> arrowHead lastDirection lastPoint
    where

        moves1 :: [ Move ]
        moves1 = shortLast moves

        shortLast :: [ Move ] -> [ Move ]
        shortLast [] = []
        shortLast [ m ]
            | dist m > 10  = [ m { dist = dist m - 10 } ]
            | otherwise    = []
        shortLast (m : ms) = m : shortLast ms

        polyPoints :: [ Point ]
        polyPoints = run start moves1

        run :: Point -> [ Move ] -> [ Point ]
        run pt []             = [ pt ]
        run pt (m :  ms)
            | dist m == 0     = run pt ms
            | otherwise       = pt : run (move (dir m) (dist m) pt) ms

        points :: String
        points = renderPolygonPoints polyPoints

        getLastDirection :: [ Move ] -> Direction
        getLastDirection [] = East
        getLastDirection [ x ] = dir x
        getLastDirection (_ : xs) = getLastDirection xs

        lastDirection :: Direction
        lastDirection = getLastDirection moves1

        getLastPoint :: [ Move ] -> Point -> Point
        getLastPoint [] pt = pt
        getLastPoint (m:ms) pt = getLastPoint ms $ move (dir m) (dist m) pt

        lastPoint :: Point
        lastPoint = getLastPoint moves start

trashCan :: Point -> Svg
trashCan org = do
    let height :: Deci
        height = 32
        width :: Deci
        width = 24
        eorg :: Point
        eorg = move North (height / 2)  org
    S.ellipse ! A.cx (deci (x eorg))
                ! A.cy (deci (y eorg))
                ! A.rx (deci (width / 2))
                ! A.ry (deci (width / 4))
                ! A.strokeWidth "2"
    let p1 = move South 2 $ move West (width / 2) eorg
        p4 = move South 2 $ move East (width / 2) eorg
        p2 = move South (height - 2) $ move East (width / 5) p1
        p3 = move South (height - 2) $ move West (width / 5) p4
        points = renderPolygonPoints [ p1, p2, p3, p4 ]
    S.polyline
        ! A.points (fromString points)
        ! A.strokeWidth "2"

origin :: Point
origin = Point 320 240

example :: Svg
example = do
    _ <- rect origin 100
    text origin "foo"
    route (move West 40 origin) [
        Move West 50,
        Move North 50,
        Move East 180,
        Move South 50,
        Move West 50 ]
    trashCan (move South 60 origin)

writeSVG :: FilePath -> Svg -> IO ()
writeSVG fname svg = do
    putStrLn $ "Writing file " ++ show fname
    writeFile fname $
        Blaze.renderSvg $
            S.docTypeSvg
                ! A.version "1.1"
                ! A.width "640"
                ! A.height "480"
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
    writeSVG (odir </> "example.svg") example
    putStrLn "Done."
