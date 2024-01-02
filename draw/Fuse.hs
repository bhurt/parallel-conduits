
module Fuse (
    fuse,
    fuseLeft
) where

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

    fuseBase :: MarkupM Fuse
    fuseBase = do
        (_, m) <- mainBox
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
        f <- fuseBase
        let (m1, m3) = splitStep (makeStep Y (c2Anchor f) (rAnchor f)) 0.4
            m2 = makeStep X (c2Anchor f) (rAnchor f)
        route (c2Anchor f) [ m1, m2, m3 ]
        trashCan (move North 60 (c1Anchor f))
        route (c1Anchor f) [ step North 40 ]

    fuseLeft :: Svg
    fuseLeft = do
        f <- fuseBase
        let (m1, m3) = splitStep (makeStep Y (c1Anchor f) (rAnchor f)) 0.4
            m2 = makeStep X (c1Anchor f) (rAnchor f)
        route (c1Anchor f) [ m1, m2, m3 ]
        trashCan (move North 60 (c2Anchor f))
        route (c2Anchor f) [ step North 40 ]


