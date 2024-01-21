
module Misc (
    proFunctor
) where

    import           MainBox
    import           Rect
    import           Render
    import           Route
    import           Text.Blaze.Svg11    (Svg)
    import           Types


    proFunctor :: Svg
    proFunctor = do
        let labels :: MainBoxLabels
            labels = defaultMainBoxLabels {
                        inputLabel = "a",
                        outputLabel = "d" }
        (_, mbox) <- mainBox'  labels
        let inner = makeRect origin 30
            forg :: Point
            forg = move West 150 origin
            gorg :: Point
            gorg = move East 150 origin
            fbox = makeBox forg 50
            gbox = makeBox gorg 50
        rectRender inner
        rectRender fbox
        rectRender gbox
        text origin "inner"
        text forg "f"
        text gorg "g"
        text (move West 90 (move North 20 origin)) "b"
        text (move East 90 (move North 20 origin)) "c"
        route (rectControl inner North)
            [ makeStep Y (rectControl inner North) (resultAnchor mbox) ]
        route (inputAnchor mbox)
            [ makeStep X (inputAnchor mbox) (rectControl fbox West) ]
        route (rectControl fbox East)
            [ makeStep X (rectControl fbox East) (rectControl inner West) ]
        route (rectControl inner East)
            [ makeStep X (rectControl inner East) (rectControl gbox West) ]
        route (rectControl gbox East)
            [ makeStep X (rectControl gbox East) (outputAnchor mbox) ]



