
module Simple (
    simple
) where

    import           MainBox
    import           Rect
    import           Route
    import           Text.Blaze.Svg11 (Svg)
    import           Types


    simple :: Svg
    simple = do
        (r, m) <- mainBox
        route (inputAnchor m)
            [ makeStep X (inputAnchor m) (rectControl r West) ]
        route (rectControl r East)
            [ makeStep X (rectControl r East) (outputAnchor m) ]
        route (rectControl r North)
            [ makeStep Y (rectControl r North) (resultAnchor m) ]
        pure ()


