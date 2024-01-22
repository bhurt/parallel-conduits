
module Data.Conduit.Parallel (
    -- * The main type
    ParConduit,

    -- * Lifting conduits
    liftC,

    -- * Fusing
    fuse,
    fuseLeft,
    fuseSemigroup,
    fuseTuple,
    fuseMap,

    -- * Parallel
    parallel

) where

    import           Data.Conduit.Parallel.Internal.Fuse
    import           Data.Conduit.Parallel.Internal.LiftC
    import           Data.Conduit.Parallel.Internal.Parallel
    import           Data.Conduit.Parallel.Internal.Type

