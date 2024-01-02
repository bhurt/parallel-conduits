
module MainBox (
    MainBox(..),
    mainBox
) where

    import           Rect
    import           Render
    import           Text.Blaze.Internal (MarkupM)
    import           Types

    data MainBox = MainBox {
                    inputAnchor :: Point,
                    outputAnchor :: Point,
                    resultAnchor :: Point  }

    mainBox :: MarkupM (Rect, MainBox)
    mainBox = do
        let r :: Rect
            r = makeRect origin 100
        rectRender r
        text (move West 280 origin) "i"
        text (move East 280 origin) "o"
        text (move North 200 origin) "r"
        pure $ (r, MainBox {
                        inputAnchor = move West 270 origin,
                        outputAnchor = move East 270 origin,
                        resultAnchor = move North 180 origin })


