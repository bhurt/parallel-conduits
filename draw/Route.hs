{-# LANGUAGE OverloadedStrings #-}

module Route (
    arrowHead,
    route
) where

    import           Data.String
    import           Render
    import           Text.Blaze.Svg11            (Svg, (!))
    import qualified Text.Blaze.Svg11            as S
    import qualified Text.Blaze.Svg11.Attributes as A
    import           Types

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

    route :: Point -> [ Step ] -> Svg
    route start moves =
            S.polyline 
                ! A.points (fromString points)
                ! A.strokeWidth "2"
            <> arrowHead lastDirection lastPoint
        where

            moves1 :: [ Step ]
            moves1 = shortLast moves

            shortLast :: [ Step ] -> [ Step ]
            shortLast [] = []
            shortLast [ m ]
                | dist m > 10  = [ m { dist = dist m - 10 } ]
                | dist m < -10 = [ m { dist = dist m + 10 } ]
                | otherwise    = []
            shortLast (m : ms) = m : shortLast ms

            polyPoints :: [ Point ]
            polyPoints = run start moves1

            run :: Point -> [ Step ] -> [ Point ]
            run pt []             = [ pt ]
            run pt (m :  ms)      = pt : run (takeStep m pt) ms

            points :: String
            points = renderPolygonPoints polyPoints

            getLastDirection :: [ Step ] -> Direction
            getLastDirection [] = East
            getLastDirection [ x ] = getDir x
            getLastDirection (_ : xs) = getLastDirection xs

            lastDirection :: Direction
            lastDirection = getLastDirection moves1

            getLastPoint :: [ Step ] -> Point -> Point
            getLastPoint [] pt = pt
            getLastPoint (m:ms) pt = getLastPoint ms $ takeStep m pt

            lastPoint :: Point
            lastPoint = getLastPoint moves start


