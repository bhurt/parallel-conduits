{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Flip
-- Description : Flip - flipping two type parameters
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
module Data.Conduit.Parallel.Internal.Flip (
    Flip(..)
) where

    import           Data.Bifoldable
    import           Data.Bifunctor
    import           Data.Bitraversable
    import           Data.Functor.Contravariant
    import           Data.Monoid
    import           Data.Profunctor


    data Flip f a b = Flip { unFlip :: f b a }

    instance Bifunctor f => Functor (Flip f a) where
        fmap g f = Flip $ first g (unFlip f)

    instance Bifunctor f => Bifunctor (Flip f) where
        bimap g h f = Flip $ bimap h g (unFlip f)
        first g f = Flip $ second g (unFlip f)
        second g f = Flip $ first g (unFlip f)

    instance Bifoldable t => Foldable (Flip t a) where
        foldMap f (Flip t) = bifoldMap f (const mempty) t
        -- TODO: fill out the implementations of the rest of Foldable.

    instance Bitraversable t => Traversable (Flip t a) where
        traverse f (Flip t) = Flip <$> bitraverse f pure t

    instance (Bifunctor f, Bifoldable f) => Bifoldable (Flip f) where
        bifold (Flip f) = getDual $ bifold (bimap Dual Dual f)
        bifoldMap f g (Flip x) = getDual $ bifold (bimap (Dual . g) (Dual . f) x)
        bifoldr f g x = bifoldl (flip g) (flip f) x . unFlip
        bifoldl f g x = bifoldr (flip g) (flip f) x . unFlip

    instance Bitraversable f => Bitraversable (Flip f) where
        bitraverse f g (Flip p) = Flip <$> bitraverse g f p

    instance Profunctor f => Contravariant (Flip f a) where
        contramap f (Flip t) = Flip $ lmap f t

