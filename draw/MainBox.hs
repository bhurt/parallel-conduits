
module MainBox (
    MainBox(..),
    MainBoxLabels(..),
    defaultMainBoxLabels,
    mainBox',
    mainBox
) where

    import           Control.Monad       (when)
    import           Rect
    import           Render
    import           Text.Blaze.Internal (MarkupM)
    import           Types

    data MainBoxLabels = MainBoxLabels {
                            inputLabel :: String,
                            outputLabel :: String,
                            resultLabel :: String }

    defaultMainBoxLabels :: MainBoxLabels
    defaultMainBoxLabels = MainBoxLabels {
                                inputLabel = "i",
                                outputLabel = "o",
                                resultLabel = "r" }

    data MainBox = MainBox {
                    inputAnchor :: Point,
                    outputAnchor :: Point,
                    resultAnchor :: Point  }

    mainBox :: MarkupM (Rect, MainBox)
    mainBox = mainBox' defaultMainBoxLabels

    mainBox' :: MainBoxLabels -> MarkupM (Rect, MainBox)
    mainBox' labels = do
        let r :: Rect
            r = makeRect origin 100
        rectRender r
        when (inputLabel labels /= "") $
            text (move West 280 origin) (inputLabel labels)
        when (outputLabel labels /= "") $
            text (move East 280 origin) (outputLabel labels)
        when (resultLabel labels /= "") $
            text (move North 200 origin) (resultLabel labels)
        pure $ (r, MainBox {
                        inputAnchor = move West 270 origin,
                        outputAnchor = move East 270 origin,
                        resultAnchor = move North 180 origin })


