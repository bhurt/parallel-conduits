{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Parallel
-- Description : The implementation of the parallel function
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
module Data.Conduit.Parallel.Internal.Parallel (
    parallel,
    parallelA
) where

    import           Data.Conduit.Parallel.Internal.Arrow
    import           Data.Conduit.Parallel.Internal.Control
    import           Data.Conduit.Parallel.Internal.Type
    import           Data.Conduit.Parallel.Internal.Worker
    import           Data.List.NonEmpty
    import           UnliftIO


    -- | Run multiple parconduits in parallel.
    --
    -- The code @parallel num inner@ creates a parconduit which
    -- internally runs @num@ copies of the @inner@ parconduit.  Pictorially,
    -- this looks like:
    --
    -- ![image](docs/parallel.svg)
    -- 
    -- Inputs are served to the different parconduits on an arbitrary
    -- basis, and outputs likewise will be intermingled.
    parallel :: forall m r i o .
                    (MonadUnliftIO m
                    , Monoid r)
                    => Int
                    -> ParConduit m r i o
                    -> ParConduit m r i o
    parallel num inner 
            | num <= 0  = error $ "Data.Conduit.Parallel.parallel called "
                                    ++ "with a non-positive number of threads!"
            | num == 1  = inner
            | otherwise = ParConduit go
        where
            go :: forall x . ReadDuct i -> WriteDuct o -> Control x m (m r)
            go rd wd = do
                addReadOpens rd (num - 1)
                addWriteOpens wd (num - 1)
                rs :: [ m r ] <-
                    sequence . Prelude.take num . Prelude.repeat
                        $ getParConduit inner rd wd
                pure $ mconcat <$> sequence rs

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
                readi :: Reader i            <- openReadDuct rdi
                wis   :: NonEmpty (Writer i) <- traverse openWriteDuct wdis
                let recur :: NonEmpty (Writer i) -> LoopM Void
                    recur ws = do
                        i <- readi
                        let (wi, ws') = case ws of
                                            y :| []     -> (y, wis)
                                            y :| (x:xs) -> (y, (x :| xs))
                        wi i
                        recur ws'
                runLoopM $ recur wis

            fuser :: WriteDuct o -> NonEmpty (ReadDuct o) -> Worker ()
            fuser wdo rdos = do
                ros :: NonEmpty (Reader o) <- traverse openReadDuct rdos
                wo  :: Writer o            <- openWriteDuct wdo
                let recur :: NonEmpty (Reader o) -> LoopM Void
                    recur rs = do
                        let (ro, rs') = case rs of
                                            y :| []     -> (y, ros)
                                            y :| (x:xs) -> (y, (x :| xs))
                        o <- ro
                        wo o
                        recur rs'
                runLoopM $ recur ros

