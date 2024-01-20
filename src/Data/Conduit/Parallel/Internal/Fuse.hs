{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Fuse
-- Description : The various ways to fuse ParConduits
-- Copyright   : (c) Brian Hurt, 2023
-- License     : BSD 3-clause
-- Maintainer  : bhurt42@gmail.com
-- Stability   : experimental
--
-- = Warning
--
-- This is an internal module of the Parallel Conduits.  You almost
-- certainly want to use [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- instead.  Anything in this module not explicitly re-exported 
-- by [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- is for internal use only, and will change or disappear without
-- notice.  Use at your own risk.
--
module Data.Conduit.Parallel.Internal.Fuse (
    fuse,
    fuseLeft,
    fuseTuple,
    fuseSemigroup,
    fuseMap
) where

    import           Control.DeepSeq
    import           Control.Exception                   (evaluate)
    import           Control.Monad.IO.Class              (MonadIO (..))
    import qualified Data.Conduit.Parallel.Internal.Duct as Duct
    import           Data.Conduit.Parallel.Internal.Type

    -- | Fuse two conduits, with a function to combine the results.
    --
    -- ![image](docs/fuseMap.svg)
    --
    fuseMap :: forall r1 r2 r m i o x .
                    (MonadIO m
                    , NFData x)
                    => (r1 -> r2 -> r)
                    -> ParConduit m r1 i x
                    -> ParConduit m r2 x o
                    -> ParConduit m r i o
    fuseMap = fuseBase forceMe
        where
            forceMe :: Duct.WriteDuct x -> Duct.WriteDuct x
            forceMe = Duct.contramapIO (evaluate . force)
                
    -- | Fuse two parallel conduits, returning the second result.
    --
    -- ![image](docs/fuse.svg)
    --
    fuse :: forall r m i o x .
                    (MonadIO m
                    , NFData x)
                    => ParConduit m () i x
                    -> ParConduit m r x o
                    -> ParConduit m r i o
    fuse = fuseMap (\() r -> r)

    -- | Fuse two parallel conduits, returning the first result.
    --
    -- ![image](docs/fuseLeft.svg)
    --
    fuseLeft :: forall r m i o x .
                    (MonadIO m
                    , NFData x)
                    => ParConduit m r i x
                    -> ParConduit m () x o
                    -> ParConduit m r i o
    fuseLeft = fuseMap (\r () -> r)

    -- | Fuse two parallel conduits, constructing a tuples of the results.
    --
    -- ![image](docs/fuseTuple.svg)
    --
    fuseTuple :: forall r1 r2 m i o x .
                    (MonadIO m
                    , NFData x)
                    => ParConduit m r1 i x
                    -> ParConduit m r2 x o
                    -> ParConduit m (r1, r2) i o
    fuseTuple = fuseMap (\r1 r2 -> (r1, r2))


    -- | Fuse two parallel conduits, appending the two results.
    --
    -- ![image](docs/fuseSemigroup.svg)
    --
    fuseSemigroup :: forall r m i o x .
                        (MonadIO m
                        , Semigroup r
                        , NFData x)
                        => ParConduit m r i x
                        -> ParConduit m r x o
                        -> ParConduit m r i o
    fuseSemigroup = fuseMap (<>)

