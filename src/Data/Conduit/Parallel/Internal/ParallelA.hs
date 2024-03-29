{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.ParallelA
-- Description : The implementation of the parallel function for ParArrows
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
module Data.Conduit.Parallel.Internal.ParallelA (
    parallelA
) where

    import           Data.Conduit.Parallel.Internal.Arrow
    import           Data.Conduit.Parallel.Internal.ParDuct
    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Conduit.Parallel.Internal.Utils
    import           Data.List.NonEmpty
    import           Data.Void
    import           UnliftIO


    data Foo m i o = Foo {
        input :: WriteDuct i,
        output :: ReadDuct o,
        waiter :: m ()
    }

    -- | Run multiple ParArrows in parallel.
    --
    -- The code @parallelA num inner@ creates a ParArrow which
    -- internally runs @num@ copies of the @inner@ ParArrow.  Pictorially,
    -- this looks like:
    --
    -- ![image](docs/parallel.svg)
    --
    parallelA :: forall m i o .
                    MonadUnliftIO m
                    => Int
                    -> ParArrow m i o
                    -> ParArrow m i o
    parallelA n pa 
            | n <= 0    = error "Parallel a called with non-positive count"
            | n == 1    = pa
            | otherwise = ParArrow go
        where
            go :: forall x .
                    ReadDuct i
                    -> WriteDuct o
                    -> Control x m (m ())
            go rdi wdo = do
                lst :: NonEmpty (Foo m i o)
                    <- traverse doSpawn (1 :| [ 2 .. n ])
                m1 <- spawnWorker $ splitter rdi (input <$> lst)
                m2 <- spawnWorker $ fuser wdo (output <$> lst)
                pure $ m1 >> m2 >> mapM_ id (waiter <$> lst)

            doSpawn :: forall x . Int -> Control x m (Foo m i o)
            doSpawn _ = do
                (rdi, wdi) :: Duct i <- newDuct
                (rdo, wdo) :: Duct o <- newDuct
                w <- getParArrow pa rdi wdo
                pure $ Foo {
                        input = wdi,
                        output = rdo,
                        waiter = w }

            splitter :: ReadDuct i -> NonEmpty (WriteDuct i) -> Worker ()
            splitter rdi wdis = do
                readi :: Reader i            <- withReadDuct rdi
                wis   :: NonEmpty (Writer i) <- traverse withWriteDuct wdis
                let recur :: NonEmpty (Writer i) -> RecurM Void
                    recur ws = do
                        i <- readi
                        let (wi, ws') = case ws of
                                            y :| []     -> (y, wis)
                                            y :| (x:xs) -> (y, (x :| xs))
                        wi i
                        recur ws'
                runRecurM $ recur wis

            fuser :: WriteDuct o -> NonEmpty (ReadDuct o) -> Worker ()
            fuser wdo rdos = do
                ros :: NonEmpty (Reader o) <- traverse withReadDuct rdos
                wo  :: Writer o            <- withWriteDuct wdo
                let recur :: NonEmpty (Reader o) -> RecurM Void
                    recur rs = do
                        let (ro, rs') = case rs of
                                            y :| []     -> (y, ros)
                                            y :| (x:xs) -> (y, (x :| xs))
                        o <- ro
                        wo o
                        recur rs'
                runRecurM $ recur ros

