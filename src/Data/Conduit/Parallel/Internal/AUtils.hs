{-# LANGUAGE InstanceSigs        #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.AUtils
-- Description : Arrow Utils
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
module Data.Conduit.Parallel.Internal.AUtils (
    runK,
    aDupe,
    aBypass
) where

    import           Control.Concurrent.STM               (retry)
    import           Control.Monad.Cont                   (ContT, lift)
    import           Control.Monad.Trans.Maybe
    import qualified Data.Conduit.Parallel.Internal.Duct  as Duct
    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Sequence                        (Seq)
    import qualified Data.Sequence                        as Seq
    import           Data.These
    import           Data.Void
    import           UnliftIO

    -- | A typedef wrapper around the core ParArrow type.
    --
    -- We don't want to define the real ParArrow type here, because
    -- we want to add all the typeclass instances in the file where
    -- we do.  But man, does using a typedef make the code a lot
    -- clearer.
    type FakeParArrow m i o = forall x .
                                Duct.ReadDuct i
                                -> Duct.WriteDuct o
                                -> ContT x m (m ())

    doRead :: forall a m . MonadIO m => IO (Maybe a) -> MaybeT m a
    doRead = MaybeT . liftIO

    doWrite :: forall a m .
                    MonadIO m
                    => (a -> IO Duct.Open)
                    -> a
                    -> MaybeT m ()
    doWrite wd a = MaybeT $ do
                                open <- liftIO $ wd a
                                case open of
                                    Duct.Open   -> pure $ Just ()
                                    Duct.Closed -> pure Nothing

    runK :: forall m i o x .
                MonadUnliftIO m
                => (i -> m o)
                -> Duct.ReadDuct i
                -> Duct.WriteDuct o
                -> ContT x m (m ())
    runK kl rd wd = spawn $ go
        where
            go :: m ()
            go = Duct.withReadDuct rd Nothing $ \r ->
                    Duct.withWriteDuct wd Nothing $ \w ->
                        let recur :: MaybeT m Void
                            recur = do
                                i <- doRead r
                                o <- lift $ kl i
                                doWrite w o
                                recur
                        in do
                            _ <- runMaybeT recur
                            pure ()

    data DupeCommand = A | B | AB | C

    aDupe :: forall i o i1 o1 i2 o2 m .
                MonadUnliftIO m
                => (i -> These i1 i2)
                -> (These o1 o2 -> o)
                -> FakeParArrow m i1 o1
                -> FakeParArrow m i2 o2
                -> FakeParArrow m i o
    aDupe splitIn fuseOut pa1 pa2 rdi wdo = do
            tvar <- liftIO $ newTVarIO Seq.empty
            (rdi1, wdi1) <- liftIO $ Duct.newDuct
            (rdo1, wdo1) <- liftIO $ Duct.newDuct
            (rdi2, wdi2) <- liftIO $ Duct.newDuct
            (rdo2, wdo2) <- liftIO $ Duct.newDuct
            m1 <- spawnIO $ splitter tvar wdi1 wdi2
            m2 <- spawnIO $ fuser tvar rdo1 rdo2
            m3 <- pa1 rdi1 wdo1
            m4 <- pa2 rdi2 wdo2
            pure $ m1 >> m2 >> m3 >> m4
        where
            splitter :: TVar (Seq DupeCommand)
                        -> Duct.WriteDuct i1
                        -> Duct.WriteDuct i2
                        -> IO ()
            splitter tvar wdi1 wdi2 = 
                flip finally (closeTVar tvar) $ do
                    Duct.withReadDuct rdi Nothing $ \ri ->
                        Duct.withWriteDuct wdi1 Nothing $ \wi1 ->
                            Duct.withWriteDuct wdi2 Nothing $ \wi2 ->
                                let recur :: MaybeT IO Void
                                    recur = do
                                        i <- doRead ri
                                        case splitIn i of
                                            This i1 -> do
                                                sendCommand tvar A
                                                doWrite wi1 i1
                                            That i2 -> do
                                                sendCommand tvar B
                                                doWrite wi2 i2
                                            These i1 i2 -> do
                                                sendCommand tvar AB
                                                doWrite wi1 i1
                                                doWrite wi2 i2
                                        recur
                                in do
                                    _ <- runMaybeT recur
                                    pure ()

            closeTVar :: TVar (Seq DupeCommand) -> IO ()
            closeTVar tvar = do
                _ <- runMaybeT $ sendCommand tvar C
                pure ()

            sendCommand :: TVar (Seq DupeCommand)
                            -> DupeCommand
                            -> MaybeT IO ()
            sendCommand tvar cmd = MaybeT . atomically $ do
                s <- readTVar tvar
                writeTVar tvar (s Seq.|> cmd)
                pure $ Just ()

            fuser :: TVar (Seq DupeCommand)
                    -> Duct.ReadDuct o1
                    -> Duct.ReadDuct o2
                    -> IO ()
            fuser tvar rdo1 rdo2 =
                Duct.withReadDuct rdo1 Nothing $ \ro1 ->
                    Duct.withReadDuct rdo2 Nothing $ \ro2 ->
                        Duct.withWriteDuct wdo Nothing $ \wo ->
                            let recur :: MaybeT IO Void
                                recur = do
                                    cmd <- readCommand tvar
                                    o <- case cmd of
                                            A -> do
                                                o1 <- doRead ro1
                                                pure . fuseOut $ This o1
                                            B -> do
                                                o2 <- doRead ro2
                                                pure . fuseOut $ That o2
                                            AB -> do
                                                o1 <- doRead ro1
                                                o2 <- doRead ro2
                                                pure . fuseOut $ These o1 o2
                                            C -> MaybeT (pure Nothing)
                                    doWrite wo o
                                    recur
                            in do
                                _ <- runMaybeT recur
                                pure ()
            readCommand :: TVar (Seq DupeCommand) -> MaybeT IO DupeCommand
            readCommand tvar = MaybeT . atomically $ do
                s <- readTVar tvar
                case Seq.viewl s of
                    Seq.EmptyL  -> retry
                    c Seq.:< s2 -> do
                        writeTVar tvar s2
                        pure $ Just c

    data Bypass a =
        Bypass a  -- A value is bypassing
        | Value   -- A value is in the inner arrow
        | Both a  -- A value is bypassing and another is in the arrow
        | Neither -- We are closing

    aBypass :: forall i o i' o' b m .
                MonadUnliftIO m
                => (i -> These b i')
                -> (These b o' -> o)
                -> FakeParArrow m i' o'
                -> FakeParArrow m i o
    aBypass splitIn fuseOut pa rdi wdo = do
            tvar :: TVar (Seq (Bypass b)) <- liftIO $ newTVarIO Seq.empty
            (rdi', wdi') <- liftIO $ Duct.newDuct
            (rdo', wdo') <- liftIO $ Duct.newDuct
            m1 <- spawnIO $ splitter wdi' tvar
            m2 <- spawnIO $ fuser rdo' tvar
            m3 <- pa rdi' wdo'
            pure $ m1 >> m2 >> m3
        where
            splitter :: Duct.WriteDuct i'
                        -> TVar (Seq (Bypass b))
                        -> IO ()
            splitter wdi' tvar =
                flip finally (closeTVar tvar) $ do
                    Duct.withReadDuct rdi Nothing $ \ri ->
                        Duct.withWriteDuct wdi' Nothing $ \wi' ->
                            let recur :: MaybeT IO Void
                                recur = do
                                    i <- doRead ri
                                    case splitIn i of
                                        This b -> putTVar tvar $ Bypass b
                                        That i' -> do
                                            putTVar tvar Value
                                            doWrite wi' i'
                                        These b i' -> do
                                            putTVar tvar $ Both b
                                            doWrite wi' i'
                                    recur
                            in do
                                _ <- runMaybeT recur
                                pure ()

            closeTVar :: TVar (Seq (Bypass b)) -> IO ()
            closeTVar tvar = do
                _ <- runMaybeT $ putTVar tvar Neither
                pure ()

            putTVar :: TVar (Seq (Bypass b)) -> Bypass b -> MaybeT IO ()
            putTVar tvar t =
                MaybeT . atomically $ do
                    s <- readTVar tvar
                    writeTVar tvar (s Seq.|> t)
                    pure $ Just ()

            fuser :: Duct.ReadDuct o'
                        -> TVar (Seq (Bypass b))
                        -> IO ()
            fuser rdo' tvar =
                Duct.withReadDuct rdo' Nothing $ \ro' ->
                    Duct.withWriteDuct wdo Nothing $ \wo ->
                        let recur :: MaybeT IO Void
                            recur = do
                                t <- getTVar tvar
                                o <- case t of
                                        This b     -> pure $ fuseOut (This b)
                                        That ()    -> do
                                            o' <- doRead ro'
                                            pure $ fuseOut (That o')
                                        These b () -> do
                                            o' <- doRead ro'
                                            pure $ fuseOut (These b o')
                                doWrite wo o
                                recur
                        in do
                            _ <- runMaybeT recur
                            pure ()

            getTVar :: TVar (Seq (Bypass b)) -> MaybeT IO (These b ())
            getTVar tvar = MaybeT . atomically $ do
                            s <- readTVar tvar
                            case Seq.viewl s of
                                Seq.EmptyL  -> retry
                                t Seq.:< s2 -> do
                                    writeTVar tvar s2
                                    pure $ 
                                        case t of
                                            Neither  -> Nothing
                                            Bypass b -> Just $ This b
                                            Value    -> Just $ That ()
                                            Both b   -> Just $ These b ()

