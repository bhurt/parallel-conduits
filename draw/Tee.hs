
module Tee (
    tee,
    merge
) where

    import           MainBox
    import           Rect
    import           Render
    import           Route
    import           Text.Blaze.Internal (MarkupM)
    import           Text.Blaze.Svg11    (Svg)
    import           Types

    data TeeData = TeeData {
                        westSplit :: Point,
                        eastMerge :: Point,
                        innerBox :: Rect
                    }

    teeBox :: String -> MarkupM TeeData
    teeBox innerName = do
        let mboxLabels = defaultMainBoxLabels {
                                outputLabel = "i"
                            }
        (_, mbox) <- mainBox' mboxLabels
        let innerPoint :: Point
            innerPoint = move North 60 origin

            innerRect :: Rect
            innerRect = makeRect innerPoint 30
        rectRender innerRect
        text innerPoint innerName

        let westPoint :: Point
            westPoint = move West 150 origin

            eastPoint :: Point
            eastPoint = move East 150 origin

            bypassPoint :: Point
            bypassPoint = move South 60 origin

        route (rectControl innerRect North)
            [   makeStep Y (rectControl innerRect North)
                            (resultAnchor mbox) ]

        route (inputAnchor mbox)
            [   makeStep X (inputAnchor mbox) westPoint,
                makeStep Y westPoint bypassPoint,
                makeStep X westPoint eastPoint,
                makeStep Y bypassPoint eastPoint,
                makeStep X eastPoint (outputAnchor mbox) ]

        return $ TeeData {
                        westSplit = westPoint,
                        eastMerge = eastPoint,
                        innerBox = innerRect }

    tee :: Svg
    tee = do
        tdata <- teeBox "sink"
        route (westSplit tdata)
            [   makeStep Y (westSplit tdata)
                    (rectControl (innerBox tdata) West),
                makeStep X (westSplit tdata)
                    (rectControl (innerBox tdata) West) ]

    merge :: Svg
    merge = do
        tdata <- teeBox "source"
        polyline (rectControl (innerBox tdata) East)
            [   makeStep X (rectControl (innerBox tdata) East)
                        (eastMerge tdata),
                makeStep Y (rectControl (innerBox tdata) East)
                        (eastMerge tdata) ]

