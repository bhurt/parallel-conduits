
module Fuse (
    fuse,
    fuseLeft,
    fuseMap,
    fuseMonoid,
    fuseTuple
) where

    import           Data.Fixed          (Deci)
    import           MainBox
    import           Rect
    import           Render
    import           Route
    import           Text.Blaze.Internal (MarkupM)
    import           Text.Blaze.Svg11    (Svg)
    import           Types

    data Fuse = Fuse {
                    c1Anchor :: Point,
                    c2Anchor :: Point,
                    rAnchor :: Point }

    fuseBase :: MainBoxLabels -> MarkupM Fuse
    fuseBase labels = do
        (_, m) <- mainBox' labels
        let r1 :: Rect
            r1 = makeRect (move West 100 origin) 30
            r2 :: Rect
            r2 = makeRect (move East 100 origin) 30
        rectRender r1
        rectRender r2
        route (inputAnchor m)
            [ makeStep X (inputAnchor m) (rectControl r1 West) ]
        route (rectControl r1 East)
            [ makeStep X (rectControl r1 East) (rectControl r2 West) ]
        route (rectControl r2 East)
            [ makeStep X (rectControl r2 East) (outputAnchor m) ]
        text (move West 100 origin) "c1"
        text (move East 100 origin) "c2"
        text (move North 20 origin) "x"
        pure $ Fuse {
                c1Anchor = rectControl r1 North,
                c2Anchor = rectControl r2 North,
                rAnchor = (resultAnchor m) }

    fuse :: Svg
    fuse = do
        f <- fuseBase defaultMainBoxLabels
        let (m1, m3) = splitStep (makeStep Y (c2Anchor f) (rAnchor f)) 0.4
            m2 = makeStep X (c2Anchor f) (rAnchor f)
        route (c2Anchor f) [ m1, m2, m3 ]
        trashCan (move North 60 (c1Anchor f))
        route (c1Anchor f) [ step North 40 ]

    fuseLeft :: Svg
    fuseLeft = do
        f <- fuseBase defaultMainBoxLabels
        let (m1, m3) = splitStep (makeStep Y (c1Anchor f) (rAnchor f)) 0.4
            m2 = makeStep X (c1Anchor f) (rAnchor f)
        route (c1Anchor f) [ m1, m2, m3 ]
        trashCan (move North 60 (c2Anchor f))
        route (c2Anchor f) [ step North 40 ]


    fuseMapBase :: String -> String -> Svg
    fuseMapBase resLabel txt = do
        f <- fuseBase (defaultMainBoxLabels { resultLabel = resLabel })
        let (m1, _) = splitStep (makeStep Y (c2Anchor f) (rAnchor f)) 0.4
            m2 = makeStep X (c2Anchor f) (rAnchor f)
            p = takeStep m1 (takeStep m2 (c2Anchor f))
            rad :: Deci
            rad = 25
        circle p rad
        let sy = makeStep Y (c2Anchor f) p
            sx = makeStep X (c2Anchor f) (move East rad p)
            sx' = makeStep X (c1Anchor f) (move West rad p)
            p2 = move North rad p
            s3 = makeStep Y p2 (rAnchor f)
        route (c1Anchor f) [ sy, sx' ]
        route (c2Anchor f) [ sy, sx ]
        route p2 [ s3 ]
        text (move North 30 (move East 20 (c2Anchor f))) "r2"
        text (move North 30 (move West 20 (c1Anchor f))) "r1"
        text p txt


    fuseMap :: Svg
    fuseMap = fuseMapBase "f r1 r2" "f"

    fuseMonoid :: Svg
    fuseMonoid = fuseMapBase "r1 <> r2" "<>"

    fuseTuple :: Svg
    fuseTuple = fuseMapBase "(r1,r2)" "(,)"

