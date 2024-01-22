
module Misc (
    proFunctor,
    parallel
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


    parallel :: Svg
    parallel = do
            let mboxLabels = defaultMainBoxLabels {
                                    resultLabel = ""
                                }
            (mbox, mboxAnchors) <- mainBox' mboxLabels
            let s :: Step
                s = makeStep Y (rectControl mbox North)
                                    (rectControl mbox South)
            let p1 = takeStep (fst (splitStep s 0.2)) (rectControl mbox North)
                p2 = takeStep (fst (splitStep s 0.4)) (rectControl mbox North)
                p3 = takeStep (fst (splitStep s 0.6)) (rectControl mbox North)
                p4 = takeStep (fst (splitStep s 0.8)) (rectControl mbox North)

                b1 = makeRect' p1 200 50
                b2 = makeRect' p2 200 50
                b4 = makeRect' p4 200 50

            rectRender b1
            rectRender b2
            rectRender b4

            text p1 "inner #1"
            text p2 "inner #2"
            text p3 "..."
            text p4 "inner #num"

            let wcp = move West 160 origin
                wp1 = takeStep (makeStep Y origin p1) wcp
                wp2 = takeStep (makeStep Y origin p2) wcp
                wp4 = takeStep (makeStep Y origin p4) wcp
                wp = takeStep (makeStep X (inputAnchor mboxAnchors) wcp)
                        (inputAnchor mboxAnchors)
            line (inputAnchor mboxAnchors) wp
            line wp1 wp4
            route wp1 [ makeStep X wp1 (rectControl b1 West) ]
            route wp2 [ makeStep X wp2 (rectControl b2 West) ]
            route wp4 [ makeStep X wp4 (rectControl b4 West) ]

            let ecp = move East 160 origin
                ep1 = takeStep (makeStep Y origin p1) ecp
                ep2 = takeStep (makeStep Y origin p2) ecp
                ep4 = takeStep (makeStep Y origin p4) ecp
                ep = takeStep (makeStep X (outputAnchor mboxAnchors) ecp)
                        (outputAnchor mboxAnchors)
            route ep [ makeStep X ep (outputAnchor mboxAnchors) ]
            line ep1 ep4
            line (rectControl b1 East) ep1
            line (rectControl b2 East) ep2
            line (rectControl b4 East) ep4

