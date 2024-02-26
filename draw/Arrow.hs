
module Arrow (
    wrapA,
    routeA
) where

    import           MainBox
    import           Rect
    import           Render
    import           Route
    import           Text.Blaze.Svg11    (Svg)
    import           Types


    wrapA :: Svg
    wrapA = do
        let labels :: MainBoxLabels
            labels = defaultMainBoxLabels {
                        inputLabel = "f i",
                        outputLabel = "f o",
                        resultLabel = "" }
        (_, mbox) <- mainBox' labels
        let inner = makeRect' origin 150 60
        rectRender inner
        text origin "inner"
        let splitPoint :: Point
            splitPoint = move West 150 origin
        text splitPoint "?"
        circle splitPoint 20
        route (move East 10 (inputAnchor mbox))
            [ makeStep X (move East 10 (inputAnchor mbox)) (move West 20 splitPoint) ]

        route (move East 20 splitPoint)
            [ makeStep X (move East 20 splitPoint) (rectControl inner West) ]

        text (move North 20 (move East 40 splitPoint)) "i"

        let mergePoint :: Point
            mergePoint = move East 150 origin

            bypassPoint :: Point
            bypassPoint = move North 80 origin

        text (move North 20 (move West 50 mergePoint)) "o"
        text (move North 25 bypassPoint) "f ()"

        text mergePoint "+"
        circle mergePoint 20

        route (rectControl inner East)
            [ makeStep X (rectControl inner East) (move West 20 mergePoint) ]

        route (move North 20 splitPoint)
            [ makeStep Y (move North 20 splitPoint) bypassPoint,
                makeStep X splitPoint mergePoint,
                makeStep Y bypassPoint (move North 20 mergePoint) ]

        route (move East 20 mergePoint)
            [ makeStep X (move East 20 mergePoint) (move West 10 (outputAnchor mbox)) ]
            

    routeA :: Svg
    routeA = do
        let labels :: MainBoxLabels
            labels = defaultMainBoxLabels {
                        inputLabel = "f a b",
                        outputLabel = "f c d",
                        resultLabel = "" }
        (_, mbox) <- mainBox' labels
        let alpha = makeRect' origin 150 60
            beta = makeRect' (move South 90 origin) 150 60
        rectRender alpha
        text origin "alpha"
        rectRender beta
        text (move South 90 origin) "beta"
        let splitPoint :: Point
            splitPoint = move West 150 origin
        text splitPoint "?"
        circle splitPoint 20
        route (move East 25 (inputAnchor mbox))
            [ makeStep X (move East 25 (inputAnchor mbox)) (move West 20 splitPoint) ]

        route (move East 20 splitPoint)
            [ makeStep X (move East 20 splitPoint) (rectControl alpha West) ]

        text (move North 20 (move East 40 splitPoint)) "a"

        route (move South 20 splitPoint)
            [ makeStep Y (move South 20 splitPoint) (rectControl beta West),
              makeStep X (move South 20 splitPoint) (rectControl beta West) ]

        text (move West 40 (move South 25 (rectControl beta West))) "b" 

        let mergePoint :: Point
            mergePoint = move East 150 origin

            bypassPoint :: Point
            bypassPoint = move North 80 origin

        text (move North 20 (move West 50 mergePoint)) "c"
        text (move North 25 bypassPoint) "f () ()"

        text mergePoint "+"
        circle mergePoint 20

        route (rectControl alpha East)
            [ makeStep X (rectControl alpha East) (move West 20 mergePoint) ]

        route (move North 20 splitPoint)
            [ makeStep Y (move North 20 splitPoint) bypassPoint,
                makeStep X splitPoint mergePoint,
                makeStep Y bypassPoint (move North 20 mergePoint) ]

        route (move East 20 mergePoint)
            [ makeStep X (move East 20 mergePoint) (move West 25 (outputAnchor mbox)) ]

        route (rectControl beta East)
            [ makeStep X (rectControl beta East) (move South 20 mergePoint),
                makeStep Y (rectControl beta East) (move South 20 mergePoint) ]
        text (move East 40 (move South 25 (rectControl beta East))) "d" 

