{-# LANGUAGE OverloadedStrings #-}

module Rect (
    Rect(..),
    makeRect',
    makeRect,
    makeBox,
    rectControl,
    rectRender
) where

    import           Data.Fixed                     (Deci)
    import           Data.String
    import           Render
    import           Text.Blaze.Svg11               (Svg, (!))
    import qualified Text.Blaze.Svg11               as S
    import qualified Text.Blaze.Svg11.Attributes    as A
    import           Types


    data Rect = Rect {
                    rectOrg :: Point,
                    xstep :: Deci,
                    ystep :: Deci }

    makeRect' :: Point -> Deci -> Deci -> Rect
    makeRect' org xscale yscale = Rect {
                                    rectOrg = org,
                                    xstep = xscale/2,
                                    ystep = yscale/2 }


    makeRect :: Point -> Deci -> Rect
    makeRect org scale = makeRect' org (scale * 4) (scale * 3)

    makeBox :: Point -> Deci -> Rect
    makeBox org scale = makeRect' org scale scale

    rectControl :: Rect -> Direction -> Point
    rectControl rect West  = move West  (xstep rect) (rectOrg rect)
    rectControl rect East  = move East  (xstep rect) (rectOrg rect)
    rectControl rect North = move North (ystep rect) (rectOrg rect)
    rectControl rect South = move South (ystep rect) (rectOrg rect)

    rectRender :: Rect -> Svg
    rectRender rect = S.polygon
                        ! A.points (fromString points)
                        ! A.strokeWidth "4"
        where
            northWest :: Point
            northWest = move North (ystep rect) $
                            move West (xstep rect) (rectOrg rect)

            northEast :: Point
            northEast = move North (ystep rect) $
                            move East (xstep rect) (rectOrg rect)

            southWest :: Point
            southWest = move South (ystep rect) $
                            move West (xstep rect) (rectOrg rect)

            southEast :: Point
            southEast = move South (ystep rect) $
                            move East (xstep rect) (rectOrg rect)

            points :: String
            points = renderPolygonPoints
                        [ northWest, northEast, southEast, southWest ]


