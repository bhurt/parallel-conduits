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
-- This is an internal module of the Parallel Conduits library.  You almost
-- certainly want to use [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- instead.  Anything in this module not explicitly re-exported 
-- by [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- is for internal use only, and will change or disappear without
-- notice.  Use at your own risk.
--
module Data.Conduit.Parallel.Internal.ParallelA (
    parallelA
) where

    import           Control.Monad.Cont
    import           Control.Monad.Trans.Maybe
    import           Data.Conduit.Parallel.Internal.Arrow
    import           Data.Conduit.Parallel.Internal.Duct
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
                    -> ContT x m (m ())
            go rdi wdo = do
                lst :: NonEmpty (Foo m i o)
                    <- traverse doSpawn (1 :| [ 2 .. n ])
                m1 <- spawnIO $ splitter rdi (input <$> lst)
                m2 <- spawnIO $ fuser wdo (output <$> lst)
                pure $ m1 >> m2 >> mapM_ id (waiter <$> lst)

            doSpawn :: forall x . Int -> ContT x m (Foo m i o)
            doSpawn _ = do
                (rdi, wdi) :: Duct i <- liftIO $ newDuct
                (rdo, wdo) :: Duct o <- liftIO $ newDuct
                w <- getParArrow pa rdi wdo
                pure $ Foo {
                        input = wdi,
                        output = rdo,
                        waiter = w }

            splitter :: ReadDuct i -> NonEmpty (WriteDuct i) -> IO ()
            splitter rdi wdis =
                flip runContT pure $ do
                    ri <- ContT $ withReadDuct rdi Nothing
                    wis :: NonEmpty (i -> IO Open)
                        <- traverse (\wdi -> ContT $ withWriteDuct wdi Nothing) wdis
                    lift $ 
                        let recur :: NonEmpty (i -> IO Open) -> MaybeT IO Void
                            recur ws = do
                                i <- readM ri
                                let (wi, ws') = case ws of
                                                    y :| []     -> (y, wis)
                                                    y :| (x:xs) -> (y, (x :| xs))
                                writeM wi i
                                recur ws'
                        in
                        runM $ recur wis

            fuser :: WriteDuct o -> NonEmpty (ReadDuct o) -> IO ()
            fuser wdo rdos =
                flip runContT pure $ do
                    ros :: NonEmpty (IO (Maybe o))
                        <- traverse (\rdo -> ContT $ withReadDuct rdo Nothing) rdos
                    wo <- ContT $ withWriteDuct wdo Nothing
                    lift $ 
                        let recur :: NonEmpty (IO (Maybe o)) -> MaybeT IO Void
                            recur rs = do
                                let (ro, rs') = case rs of
                                                    y :| []     -> (y, ros)
                                                    y :| (x:xs) -> (y, (x :| xs))
                                o <- readM ro
                                writeM wo o
                                recur rs'
                        in
                        runM $ recur ros

