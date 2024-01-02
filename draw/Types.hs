
module Types (
    Point(..),
    Direction(..),
    Axis(..),
    Step(..),
    step,
    makeStep,
    takeStep,
    getDir,
    move,
    rot,
    rev,
    splitStep
) where

    import           Data.Fixed (Deci)

    data Point = Point {
                    x :: Deci,
                    y :: Deci }

    -- This used to be:
    --     data Direction = Up | Down | Left | Right
    -- but Left and Right conflicted with Either's Left and Right.
    data Direction = North | South | West | East

    data Axis = X | Y

    data Step = Step {
                    axis :: Axis,
                    dist :: Deci }

    step :: Direction -> Deci -> Step
    step North d = Step Y (negate d)
    step South d = Step Y d
    step West  d = Step X (negate d)
    step East  d = Step X d

    takeStep :: Step -> Point -> Point
    takeStep st p =
        case axis st of
            X -> p { x = x p + dist st }
            Y -> p { y = y p + dist st }

    getDir :: Step -> Direction
    getDir st =
        case axis st of
            X
                | dist st > 0 -> East
                | otherwise   -> West
            Y
                | dist st > 0 -> South
                | otherwise   -> North

    makeStep :: Axis -> Point -> Point -> Step
    makeStep X start stop = Step X (x stop - x start)
    makeStep Y start stop = Step Y (y stop - y start)

    move :: Direction -> Deci -> Point -> Point
    move dir dis = takeStep $ step dir dis

    -- rotate 90 degrees counter-clockwise.
    rot :: Direction -> Direction
    rot North = West
    rot West  = South
    rot South = East
    rot East  = North

    -- rotate 180 degrees
    rev :: Direction -> Direction
    rev North = South
    rev West  = East
    rev South = North
    rev East  = West

    splitStep :: Step -> Deci -> (Step, Step)
    splitStep m x =
        let d = (dist m) * x  in
        (m { dist = d }, m { dist = dist m - d })


