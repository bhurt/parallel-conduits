{-# LANGUAGE OverloadedStrings #-}

module Render (
    showDeci,
    origin,
    renderPolygonPoints,
    text,
    text',
    circle,
    trashCan
) where

    import           Data.Fixed                  (Deci)
    import           Data.List                   (intercalate)
    import           Data.String
    import           Text.Blaze.Svg11            (Svg, (!))
    import qualified Text.Blaze.Svg11            as S
    import qualified Text.Blaze.Svg11.Attributes as A
    import           Types

    showDeci :: (IsString s) => Deci -> s
    showDeci = fromString . fixString . show
        where
            fixString :: String -> String
            fixString []     = []
            fixString ".0"   = ""
            fixString (x:xs) = x : fixString xs


    renderPolygonPoints :: [ Point ] -> String
    renderPolygonPoints pts = intercalate " " $ renderPoint <$> pts
        where
            renderPoint :: Point -> String
            renderPoint pt = showDeci (x pt) ++ "," ++ showDeci (y pt)


    text :: Point -> String -> Svg
    text org txt = S.text_
                        ! A.x (showDeci (x org))
                        ! A.y (showDeci (y org))
                        ! A.fill "black"
                        $ S.text (fromString txt)

    text' :: Deci -> Point -> String -> Svg
    text' fsize org txt = S.text_
                        ! A.x (showDeci (x org))
                        ! A.y (showDeci (y org))
                        ! A.fill "black"
                        ! A.fontSize (showDeci fsize)
                        $ S.text (fromString txt)

    origin :: Point
    origin = Point 320 240


    circle :: Point -> Deci -> Svg
    circle org rad = S.circle
                        ! A.cx (showDeci (x org))
                        ! A.cy (showDeci (y org))
                        ! A.r (showDeci rad)

    trashCan :: Point -> Svg
    trashCan org = do
        let height :: Deci
            height = 32
            width :: Deci
            width = 24
            eorg :: Point
            eorg = move North (height / 2)  org
        S.ellipse ! A.cx (showDeci (x eorg))
                    ! A.cy (showDeci (y eorg))
                    ! A.rx (showDeci (width / 2))
                    ! A.ry (showDeci (width / 4))
                    ! A.strokeWidth "2"
        let p1 = move South 2 $ move West (width / 2) eorg
            p4 = move South 2 $ move East (width / 2) eorg
            p2 = move South (height - 2) $ move East (width / 5) p1
            p3 = move South (height - 2) $ move West (width / 5) p4
            points = renderPolygonPoints [ p1, p2, p3, p4 ]
        S.polyline
            ! A.points (fromString points)
            ! A.strokeWidth "2"


