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

    import           Control.Monad.Cont
    import           Control.Monad.Trans.Maybe
    import           Data.Bitraversable
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Utils
    import           Data.Foldable                        (traverse_)
    import           Data.Void

    copier :: forall a .
                ReadDuct a
                -> WriteDuct a
                -> IO ()
    copier src snk = do
        withReadDuct src Nothing $ \ra ->
            withWriteDuct snk Nothing $ \wa ->
                let recur :: MaybeT IO Void
                    recur = do
                        a <- readM ra
                        writeM wa a
                        recur
                in
                runM recur

    direct :: forall a b f .
                Bitraversable f
                => ReadDuct (f a b)
                -> WriteDuct a
                -> WriteDuct b
                -> IO ()
    direct rdf wda wdb = do
        withReadDuct rdf Nothing $ \rf ->
            withWriteDuct wda Nothing $ \wa ->
                withWriteDuct wdb Nothing $ \wb ->
                    let recur :: MaybeT IO Void
                        recur = do
                            f <- readM rf
                            _ <- bitraverse
                                    (writeM wa)
                                    (writeM wb)
                                    f
                            recur
                    in
                    runM recur

    duplicator :: forall a f .
                    Traversable f
                    => ReadDuct a
                    -> f (WriteDuct a)
                    -> IO ()
    duplicator rda fwda = runContT go fini
        where
            go :: ContT () IO (IO (Maybe a), f (a -> IO Open))
            go = do
                ra <- ContT $ withReadDuct rda Nothing
                fwa <- traverse (\wda -> ContT $ withWriteDuct wda Nothing) fwda
                pure (ra, fwa)

            fini :: (IO (Maybe a), f (a -> IO Open)) -> IO ()
            fini (ra, fwa) =
                let recur :: MaybeT IO Void
                    recur = do
                        a <- readM ra
                        traverse_ (flip writeM a) fwa
                        recur
                in
                runM recur
            
    traverser :: forall f a .
                    Traversable f
                    => ReadDuct (f a)
                    -> WriteDuct a
                    -> IO ()
    traverser rdf wda = 
        withReadDuct rdf Nothing $ \rf ->
            withWriteDuct wda Nothing $ \wa ->
                let recur :: MaybeT IO Void
                    recur = do
                        f <- readM rf
                        traverse_ (writeM wa) f
                        recur
                in
                runM recur
