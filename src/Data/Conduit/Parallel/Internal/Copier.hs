{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Copier
-- Description : Code to copy values from a read duct to 1 or more write ducts.
-- Copyright   : (c) Brian Hurt, 2024
-- License     : BSD 3-clause
-- Maintainer  : bhurt42@gmail.com
-- Stability   : experimental
--
-- = Warning
--
-- This is an internal module of the Parallel Conduits library.  You
-- almost certainly want to use "Data.Conduit.Parallel"  instead.  
-- Anything in this module not explicitly re-exported by
-- "Data.Conduit.Parallel" is for internal use only, and will change
-- or disappear without notice.  Use at your own risk.
--
module Data.Conduit.Parallel.Internal.Copier (
    copier,
    duplicator,
    direct,
    traverser
) where

    import           Data.Bitraversable
    import           Data.Conduit.Parallel.Internal.ParDuct
    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Conduit.Parallel.Internal.Utils
    import           Data.Foldable                          (traverse_)
    import           Data.Void

    copier :: forall a .
                ReadDuct a
                -> WriteDuct a
                -> Worker ()
    copier src snk = do
        reada :: Reader a <- withReadDuct src
        writea :: Writer a <- withWriteDuct snk
        let recur :: RecurM Void
            recur = do
                a <- reada
                writea a
                recur
        runRecurM recur

    direct :: forall a b f .
                Bitraversable f
                => ReadDuct (f a b)
                -> WriteDuct a
                -> WriteDuct b
                -> Worker ()
    direct rdf wda wdb = do
        readf  :: Reader (f a b) <- withReadDuct rdf
        writea :: Writer a       <- withWriteDuct wda
        writeb :: Writer b       <- withWriteDuct wdb
        let recur :: RecurM Void
            recur = do
                f :: f a b <- readf
                _ <- bitraverse writea writeb f
                recur
        runRecurM recur

    duplicator :: forall a f .
                    Traversable f
                    => ReadDuct a
                    -> f (WriteDuct a)
                    -> Worker ()
    duplicator rda fwda = do
        reada   :: Reader a     <- withReadDuct rda
        writesf :: f (Writer a) <- traverse withWriteDuct fwda
        let recur :: RecurM Void
            recur = do
                a :: a <- reada
                traverse_ ($ a) writesf
                recur
        runRecurM recur

    traverser :: forall f a .
                    Traversable f
                    => ReadDuct (f a)
                    -> WriteDuct a
                    -> Worker ()
    traverser rdf wda = do
        readfa :: Reader (f a) <- withReadDuct rdf
        writea :: Writer a     <- withWriteDuct wda
        let recur :: RecurM Void
            recur = do
                f :: f a <- readfa
                traverse_ writea f
                recur
        runRecurM recur

