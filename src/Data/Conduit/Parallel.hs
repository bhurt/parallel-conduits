-- |
-- Module      : Data.Conduit.Parallel
-- Description : Multi-threaded library based on conduits.
-- Copyright   : (c) Brian Hurt, 2024
-- License     : BSD 3-clause
-- Maintainer  : bhurt42@gmail.com
-- Stability   : experimental
--

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

    -- * Routing
    parallel,
    tee,
    merge,
    routeEither,
    routeThese,
    routeTuple,
    fixP

) where

    import           Data.Conduit.Parallel.Internal.Circuit
    import           Data.Conduit.Parallel.Internal.Fuse
    import           Data.Conduit.Parallel.Internal.LiftC
    import           Data.Conduit.Parallel.Internal.Parallel
    import           Data.Conduit.Parallel.Internal.Type
    import           Data.Conduit.Parallel.Internal.Tee

