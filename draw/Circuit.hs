
module Circuit (
    routeEither,
    routeThese,
    routeTuple,
    fixP
) where

    import           MainBox
    import           Rect
    import           Render
    import           Route
    import           Text.Blaze.Internal (MarkupM)
    import           Text.Blaze.Svg11    (Svg)
    import           Types

    data Base = Base {
                    splitPoint :: Point,
                    inputPoint :: Point
                }

    baseRoute :: MarkupM Base
    baseRoute = do
        let mboxLabels = defaultMainBoxLabels {
                                inputLabel = "",
                                outputLabel = "c",
                                resultLabel = ""
                            }
        (_, mbox) <- mainBox' mboxLabels
        let alphaOrg = move North 60 origin
            betaOrg = move South 60 origin
            splitPt = move West 140 origin
            joinPt = move East 140 origin

            alphaRect = makeRect alphaOrg 30
            betaRect = makeRect betaOrg 30

        rectRender alphaRect
        rectRender betaRect
        text alphaOrg "alpha"
        text betaOrg "beta"

        circle splitPt 20
        text splitPt "?"


        let sp2 = move North 20 splitPt
            cp1 = rectControl alphaRect West
        route sp2 [ makeStep Y sp2 cp1, makeStep X sp2 cp1 ]

        let sp3 = move South 20 splitPt
            cp2 = rectControl betaRect West
        route sp3 [ makeStep Y sp3 cp2, makeStep X sp3 cp2 ]

        text (move West 40 (move North 20 cp1)) "a"
        text (move West 40 (move South 20 cp2)) "b"

        let sp4 = rectControl alphaRect East
        polyline sp4 [ makeStep X sp4 joinPt, makeStep Y sp4 joinPt ]

        let sp5 = rectControl betaRect East
        polyline sp5 [ makeStep X sp5 joinPt, makeStep Y sp5 joinPt ]

        route joinPt [ makeStep X joinPt (outputAnchor mbox) ]
        pure $ Base {
                splitPoint = move West 20 splitPt,
                inputPoint = move West 20 (inputAnchor mbox) }


    routeEither :: Svg
    routeEither = do
        base <- baseRoute
        text' 20  (move East 30 (inputPoint base)) "Either a b"
        let pt1 = move East 75 $ inputPoint base
        route pt1 [ makeStep X pt1 (splitPoint base) ]

    routeThese :: Svg
    routeThese = do
        base <- baseRoute
        text' 20  (move East 30 (inputPoint base)) "These a b"
        let pt1 = move East 75 $ inputPoint base
        route pt1 [ makeStep X pt1 (splitPoint base) ]

    routeTuple :: Svg
    routeTuple = do
        base <- baseRoute
        text' 20  (move East 30 (inputPoint base)) "(a, b)"
        let pt1 = move East 75 $ inputPoint base
        route pt1 [ makeStep X pt1 (splitPoint base) ]


    fixP :: Svg
    fixP = do
        (_, mbox) <- mainBox
        let innerRect = makeRect origin 30
        rectRender innerRect
        text origin "inner"
        let p1 = inputAnchor mbox
            p2 = rectControl innerRect West
        route p1 [ makeStep X p1 p2 ]
        let splitPoint = move East 140 origin
        text splitPoint "?"
        circle splitPoint 20
        let p3 = rectControl innerRect East
            p4 = move West 20 splitPoint
        route p3 [ makeStep X p3 p4 ]
        let p5 = move East 20 splitPoint
            p6 = outputAnchor mbox
        route p5 [ makeStep X p5 p6 ]
        let p7 = rectControl innerRect North
            p8 = resultAnchor mbox
        route p7 [ makeStep Y p7 p8 ]
        let p9 = move South 20 splitPoint
            p10 = move South 90 origin
            joinPoint = move West 140 origin
        polyline p9
            [ makeStep Y p9 p10,
                makeStep X p9 joinPoint,
                makeStep Y p10 joinPoint ]
        text (move South 25 p10) "i"
