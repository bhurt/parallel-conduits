{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Tee
-- Description : tee, merge, and other routing combinators.
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
module Data.Conduit.Parallel.Internal.Tee (
    tee
    , merge
    , teeMany
    , mergeMany
) where

    import           Control.Applicative                    (liftA2)
    import           Data.Conduit.Parallel.Internal.Control
    import           Data.Conduit.Parallel.Internal.Type
    import           Data.Conduit.Parallel.Internal.Worker
    import           Data.Foldable                          (traverse_)
    import           Data.List.NonEmpty                     (NonEmpty (..))
    import qualified Data.List.NonEmpty                     as NE
    import           UnliftIO

    -- | Copy values into multiple sinks.
    --
    teeMany :: forall r m i .
                (MonadUnliftIO m
                , Semigroup r)
                => NonEmpty (ParConduit m r i Void)
                -> ParConduit m r i i
    teeMany sinks = ParConduit go
        where
            go :: forall x .
                    ReadDuct i
                    -> WriteDuct i
                    -> Control x m (m r)
            go rdi wdi = do
                wds :: NonEmpty (WriteDuct i, m r) <- traverse start sinks
                mu :: m () <- spawnWorker $
                                duplicator rdi (NE.cons wdi (fst <$> wds))
                let x :: m r
                    xs :: [ m r ]
                    (x :| xs) = snd <$> wds
                    mr :: m r
                    mr = foldl (liftA2 (<>)) x xs
                pure $ mu >> mr

            start :: forall x .
                        ParConduit m r i Void
                        -> Control x m (WriteDuct i, m r)
            start pc = do
                (rdi, wdi) :: Duct i <- newDuct
                let wdv :: WriteDuct Void
                    (_, wdv) = newClosedDuct
                mr :: m r <- getParConduit pc rdi wdv
                pure (wdi, mr)



    -- | Copy values into a sink.
    --
    -- Pictorially, @tee inner@ looks like:
    --
    -- ![image](docs/tee.svg)
    -- 
    tee :: forall r m i .
            MonadUnliftIO m
            => ParConduit m r i Void
            -> ParConduit m r i i
    tee sink = ParConduit go
        where
            -- Note that defining tee sink = teeMany (NE.singleton sink)
            -- doesn't work, because it requires a "pointless" Semigroup
            -- dependency on r.
            go :: forall x .
                    ReadDuct i
                    -> WriteDuct i
                    -> Control x m (m r)
            go rdi wdi = do
                (rdi', wdi') :: Duct i <- newDuct
                let wdv :: WriteDuct Void
                    (_, wdv) = newClosedDuct
                mr :: m r <- getParConduit sink rdi' wdv
                mu :: m () <- spawnWorker $ duplicator rdi [ wdi, wdi' ]
                pure $ mu >> mr


    -- | Merge values from multiple sources
    --
    mergeMany :: forall r m o .
                    (MonadUnliftIO m
                    , Semigroup r)
                    => NonEmpty (ParConduit m r () o)
                    -> ParConduit m r o o
    mergeMany sources = ParConduit go
        where
            go :: forall x .
                    ReadDuct o
                    -> WriteDuct o
                    -> Control x m (m r)
            go rd wd = do
                addWriteOpens wd (NE.length sources)
                mu :: m () <- spawnWorker $ copier rd wd
                let crd :: ReadDuct ()
                    (crd, _) = newClosedDuct
                (x :| xs) :: NonEmpty (m r)
                    <- traverse (\pc -> getParConduit pc crd wd) sources
                let mr :: m r
                    mr = foldl (liftA2 (<>)) x xs
                pure $ mu >> mr


    -- | Merge in values from a source.
    --
    -- Pictorially, @merge source@ looks like:
    --
    -- ![image](docs/merge.svg)
    --
    merge :: forall r m o .
                MonadUnliftIO m
                => ParConduit m r () o
                -> ParConduit m r o  o
    merge source = ParConduit go
        where
            -- Again, defining merge source = mergeMany (NE.singleton source)
            -- doesn't work, because it requires a "pointless" Semigroup
            -- dependency on r.
            go :: forall x .
                    ReadDuct o
                    -> WriteDuct o
                    -> Control x m (m r)
            go rd wd = do
                addWriteOpens wd 1
                mu :: m () <- spawnWorker $ copier rd wd
                let crd :: ReadDuct ()
                    (crd, _) = newClosedDuct
                mr :: m r <- getParConduit source crd wd
                pure $ mu >> mr

    duplicator :: forall a f .
                    Traversable f
                    => ReadDuct a
                    -> f (WriteDuct a)
                    -> Worker ()
    duplicator rda fwda = do
        reada   :: Reader a     <- openReadDuct rda
        writesf :: f (Writer a) <- traverse openWriteDuct fwda
        let recur :: LoopM Void
            recur = do
                a :: a <- reada
                traverse_ ($ a) writesf
                recur
        runLoopM recur

