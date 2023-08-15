{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Type
-- Description : The main parallel conduit type
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
module Data.Conduit.Parallel.Internal.Type (
    ParConduit(..)
) where

    import           Control.Monad.Cont                  (ContT)
    import qualified Data.Conduit.Parallel.Internal.Duct as Duct
    import qualified Data.Functor.Contravariant          as Contra
    import qualified Data.Profunctor                     as Pro

    newtype ParConduit m r i o = ParConduit {
                                    getParConduit :: 
                                        forall x .
                                        Duct.ReadDuct i
                                        -> Duct.WriteDuct o
                                        -> ContT x m (m r) }

    instance Functor (ParConduit m r i) where
        fmap f c = ParConduit $
                    \rd wd -> getParConduit c rd (Contra.contramap f wd)


    instance Pro.Profunctor (ParConduit m r) where
        dimap f g c = ParConduit $
                        \rd wd ->
                            getParConduit c
                                (fmap f rd)
                                (Contra.contramap g wd)
        lmap f c = ParConduit $
                    \rd wd ->
                        getParConduit c
                            (fmap f rd)
                            wd

        rmap g c = ParConduit $
                    \rd wd ->
                        getParConduit c
                            rd
                            (Contra.contramap g wd)
    
