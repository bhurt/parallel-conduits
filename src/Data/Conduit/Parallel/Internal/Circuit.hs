{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Circuit
-- Description : Directing values into sub-conduits
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
module Data.Conduit.Parallel.Internal.Circuit (
    routeEither,
    routeThese,
    routeTuple,
    fixP
) where

    import           Control.Monad.Cont
    import           Data.Conduit.Parallel.Internal.Copier
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Conduit.Parallel.Internal.Type
    import           Data.These
    import           UnliftIO

    router :: forall m a b c .
                MonadUnliftIO m
                => (a -> These b c)
                -> ReadDuct a
                -> WriteDuct b
                -> WriteDuct c
                -> m ()
    router f rda wrb wrc = do
            ma <- liftIO $ readDuct rda Nothing
            case ma of
                Nothing -> closeAll
                Just a  ->
                    case f a of
                        This b    -> doWrite wrb b (router f rda wrb wrc)
                        That c    -> doWrite wrc c (router f rda wrb wrc)
                        These b c ->
                            doWrite wrb b
                                (doWrite wrc c (router f rda wrb wrc))
        where
            closeAll :: m ()
            closeAll = liftIO $ do
                _ <- closeReadDuct rda
                closeWriteDuct wrb
                closeWriteDuct wrc
                pure ()

            doWrite :: forall y . WriteDuct y -> y -> m () -> m ()
            doWrite wdy y next = do
                r <- liftIO $ writeDuct wdy Nothing y
                case r of
                    Open -> next
                    Closed -> closeAll
                        
    baseRoute :: forall m a b c d r .
                    (MonadUnliftIO m
                    , Semigroup r)
                    => (a -> These b c)
                    -> ParConduit m r b d
                    -> ParConduit m r c d
                    -> ParConduit m r a d
    baseRoute f bcond ccond = ParConduit go
        where
            go :: forall x .
                    ReadDuct a
                    -> WriteDuct d
                    -> ContT x m (m r)
            go rda wdd = do
                (rdb, wrb) <- liftIO $ newDuct
                (rdc, wrc) <- liftIO $ newDuct
                mr1 <- getParConduit bcond rdb wdd
                mr2 <- getParConduit ccond rdc wdd
                mu  <- spawn $ router f rda wrb wrc
                pure $ do
                    () <- mu
                    r1 <- mr1
                    r2 <- mr2
                    pure $ r1 <> r2

    -- | Route to either of two sub-parconduits.
    --
    -- ![image](docs/routeEither.svg)
    -- 
    routeEither :: forall m a b c r .
                    (MonadUnliftIO m
                    , Semigroup r)
                    => ParConduit m r a c
                    -> ParConduit m r b c
                    -> ParConduit m r (Either a b) c
    routeEither = baseRoute f
        where
            f :: Either a b -> These a b
            f (Left a)  = This a
            f (Right b) = That b

    -- | Route to either or both of two sub-parconduits.
    --
    -- ![image](docs/routeThese.svg)
    -- 
    routeThese :: forall m a b c r .
                    (MonadUnliftIO m
                    , Semigroup r)
                    => ParConduit m r a c
                    -> ParConduit m r b c
                    -> ParConduit m r (These a b) c
    routeThese = baseRoute id

    -- | Route to both of two sub-parconduits.
    --
    -- ![image](docs/routeTuple.svg)
    -- 
    routeTuple :: forall m a b c r .
                    (MonadUnliftIO m
                    , Semigroup r)
                    => ParConduit m r a c
                    -> ParConduit m r b c
                    -> ParConduit m r (a, b) c
    routeTuple = baseRoute f
        where
            f :: (a, b) -> These a b
            f (a, b) = These a b

    -- | Feed outputs back in as inputs.
    --
    -- ![image](docs/fixP.svg)
    -- 
    fixP :: forall m i o r .
                MonadUnliftIO m
                => ParConduit m r i (Either i o)
                -> ParConduit m r i o
    fixP inner = ParConduit go
        where
            go :: forall x .
                    ReadDuct i
                    -> WriteDuct o
                    -> ContT x m (m r)
            go rdi wro = do
                (rdi', wri') <- liftIO $ newDuct
                (rde, wre) <- liftIO $ newDuct
                mr <- getParConduit inner rdi' wre
                mcp :: m () <- spawn $ copier rdi wri' []
                mrt :: m () <- spawn $ router f rde wri' wro
                pure $ mrt >> mcp >> mr

            f :: Either i o -> These i o
            f (Left i)  = This i
            f (Right o) = That o
