-- |
-- Module      : Data.Conduit.Parallel
-- Description : Multi-threaded library based on conduits.
-- Copyright   : (c) Brian Hurt, 2024
-- License     : BSD 3-clause
-- Maintainer  : bhurt42@gmail.com
-- Stability   : experimental
--

module Data.Conduit.Parallel (
    -- * ParConduits

    ParConduit,

    -- ** Creating conduits
    liftC,
    forceC,

    -- ** Fusing
    fuse,
    fuseLeft,
    fuseSemigroup,
    fuseTuple,
    fuseMap,

    -- ** Routing
    parallel,
    tee,
    merge,
    route,
    fixP,

    -- ** Caching
    cache,

    -- * ParArrows
    ParArrow,
    toParConduit,
    liftK,
    wrapA,
    routeA,
    parallelA,
    forceA,

    -- * Misc
    Flip(..)
) where

    import           Data.Conduit.Parallel.Internal.Arrow
    import           Data.Conduit.Parallel.Internal.Cache
    import           Data.Conduit.Parallel.Internal.Circuit
    import           Data.Conduit.Parallel.Internal.Flip
    import           Data.Conduit.Parallel.Internal.LiftC
    import           Data.Conduit.Parallel.Internal.Parallel
    import           Data.Conduit.Parallel.Internal.ParallelA
    import           Data.Conduit.Parallel.Internal.Type
    import           Data.Conduit.Parallel.Internal.Tee

