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
-- This is an internal module of the Parallel Conduits library.  You almost
-- certainly want to use [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- instead.  Anything in this module not explicitly re-exported 
-- by [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- is for internal use only, and will change or disappear without
-- notice.  Use at your own risk.
--
module Data.Conduit.Parallel.Internal.Copier (
    copier,
    duplicator,
    direct,
    traverser
) where

    import           Control.Monad.Trans.Maybe
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
        ra <- withReadDuct src
        wa <- withWriteDuct snk
        let recur :: MaybeT IO Void
            recur = do
                a <- readM ra
                writeM wa a
                recur
        runM recur

    direct :: forall a b f .
                Bitraversable f
                => ReadDuct (f a b)
                -> WriteDuct a
                -> WriteDuct b
                -> Worker ()
    direct rdf wda wdb = do
        rf <- withReadDuct rdf
        wa <- withWriteDuct wda
        wb <- withWriteDuct wdb
        let recur :: MaybeT IO Void
            recur = do
                f <- readM rf
                _ <- bitraverse
                        (writeM wa)
                        (writeM wb)
                        f
                recur
        runM recur

    duplicator :: forall a f .
                    Traversable f
                    => ReadDuct a
                    -> f (WriteDuct a)
                    -> Worker ()
    duplicator rda fwda = do
        ra <- withReadDuct rda
        fwa <- traverse withWriteDuct fwda
        let recur :: MaybeT IO Void
            recur = do
                a <- readM ra
                traverse_ (flip writeM a) fwa
                recur
        runM recur
            
    traverser :: forall f a .
                    Traversable f
                    => ReadDuct (f a)
                    -> WriteDuct a
                    -> Worker ()
    traverser rdf wda = do
        rf <- withReadDuct rdf
        wa <- withWriteDuct wda
        let recur :: MaybeT IO Void
            recur = do
                f <- readM rf
                traverse_ (writeM wa) f
                recur
        runM recur
