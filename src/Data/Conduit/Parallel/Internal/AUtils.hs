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
    aBypass,
    aLoop
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
    import           System.IO.Unsafe                     (unsafePerformIO)
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

    type Queue a = TVar (Seq (Maybe a))

    makeQueue :: forall m a .
                    MonadIO m
                    => m (Queue a)
    makeQueue = liftIO $ newTVarIO (Seq.empty)

    withCloseQueue :: forall m a b .
                        MonadUnliftIO m
                        => Queue a
                        -> m b
                        -> m b
    withCloseQueue que act = finally act doClose
        where
            doClose :: m ()
            doClose = liftIO . atomically $ do
                        modifyTVar que (\s -> s Seq.|> Nothing)

    queueAdd :: forall a .  Queue a -> a -> MaybeT IO ()
    queueAdd que a = MaybeT . atomically $ do
        modifyTVar que (\s -> s Seq.|> Just a)
        pure $ Just ()

    queueGet :: forall a .  Queue a -> MaybeT IO a
    queueGet que = MaybeT . atomically $ do
        s <- readTVar que
        case Seq.viewl s of
            Seq.EmptyL  -> retry
            (Just a) Seq.:< s2 -> do
                writeTVar que s2
                pure $ Just a
            Nothing Seq.:< _   -> do
                pure Nothing

    data DupeCommand = A | B | AB

    aDupe :: forall i o i1 o1 i2 o2 m .
                MonadUnliftIO m
                => (i -> These i1 i2)
                -> (These o1 o2 -> o)
                -> FakeParArrow m i1 o1
                -> FakeParArrow m i2 o2
                -> FakeParArrow m i o
    aDupe splitIn fuseOut pa1 pa2 rdi wdo = do
            que :: Queue DupeCommand <- makeQueue
            (rdi1, wdi1) <- liftIO $ Duct.newDuct
            (rdo1, wdo1) <- liftIO $ Duct.newDuct
            (rdi2, wdi2) <- liftIO $ Duct.newDuct
            (rdo2, wdo2) <- liftIO $ Duct.newDuct
            m1 <- spawnIO $ splitter que wdi1 wdi2
            m2 <- spawnIO $ fuser que rdo1 rdo2
            m3 <- pa1 rdi1 wdo1
            m4 <- pa2 rdi2 wdo2
            pure $ m1 >> m2 >> m3 >> m4
        where
            splitter :: Queue DupeCommand
                        -> Duct.WriteDuct i1
                        -> Duct.WriteDuct i2
                        -> IO ()
            splitter que wdi1 wdi2 = 
                withCloseQueue que $ do
                    Duct.withReadDuct rdi Nothing $ \ri ->
                        Duct.withWriteDuct wdi1 Nothing $ \wi1 ->
                            Duct.withWriteDuct wdi2 Nothing $ \wi2 ->
                                let recur :: MaybeT IO Void
                                    recur = do
                                        i <- doRead ri
                                        case splitIn i of
                                            This i1 -> do
                                                queueAdd que A
                                                doWrite wi1 i1
                                            That i2 -> do
                                                queueAdd que B
                                                doWrite wi2 i2
                                            These i1 i2 -> do
                                                queueAdd que AB
                                                doWrite wi1 i1
                                                doWrite wi2 i2
                                        recur
                                in do
                                    _ <- runMaybeT recur
                                    pure ()

            fuser :: Queue DupeCommand
                    -> Duct.ReadDuct o1
                    -> Duct.ReadDuct o2
                    -> IO ()
            fuser que rdo1 rdo2 =
                Duct.withReadDuct rdo1 Nothing $ \ro1 ->
                    Duct.withReadDuct rdo2 Nothing $ \ro2 ->
                        Duct.withWriteDuct wdo Nothing $ \wo ->
                            let recur :: MaybeT IO Void
                                recur = do
                                    cmd <- queueGet que
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
                                    doWrite wo o
                                    recur
                            in do
                                _ <- runMaybeT recur
                                pure ()

    aBypass :: forall i o i' o' b m .
                MonadUnliftIO m
                => (i -> These b i')
                -> (These b o' -> o)
                -> FakeParArrow m i' o'
                -> FakeParArrow m i o
    aBypass splitIn fuseOut pa rdi wdo = do
            que :: Queue (These b ()) <- makeQueue
            (rdi', wdi') <- liftIO $ Duct.newDuct
            (rdo', wdo') <- liftIO $ Duct.newDuct
            m1 <- spawnIO $ splitter wdi' que
            m2 <- spawnIO $ fuser rdo' que
            m3 <- pa rdi' wdo'
            pure $ m1 >> m2 >> m3
        where
            splitter :: Duct.WriteDuct i'
                        -> Queue (These b ())
                        -> IO ()
            splitter wdi' que =
                withCloseQueue que $ do
                    Duct.withReadDuct rdi Nothing $ \ri ->
                        Duct.withWriteDuct wdi' Nothing $ \wi' ->
                            let recur :: MaybeT IO Void
                                recur = do
                                    i <- doRead ri
                                    case splitIn i of
                                        This b -> queueAdd que $ This b
                                        That i' -> do
                                            queueAdd que $ That ()
                                            doWrite wi' i'
                                        These b i' -> do
                                            queueAdd que $ These b ()
                                            doWrite wi' i'
                                    recur
                            in do
                                _ <- runMaybeT recur
                                pure ()

            fuser :: Duct.ReadDuct o'
                        -> Queue (These b ())
                        -> IO ()
            fuser rdo' que =
                Duct.withReadDuct rdo' Nothing $ \ro' ->
                    Duct.withWriteDuct wdo Nothing $ \wo ->
                        let recur :: MaybeT IO Void
                            recur = do
                                t <- queueGet que
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

    aLoop :: forall m b c d .
                MonadUnliftIO m
                => FakeParArrow m (b, d) (c, d)
                -> FakeParArrow m b c
    aLoop inner rd wd  = do
            que :: Queue (IORef (Maybe d)) <- makeQueue
            (rdbd, wdbd) <- liftIO $ Duct.newDuct
            (rdcd, wdcd) <- liftIO $ Duct.newDuct
            m1 <- spawnIO $ prefix que wdbd
            m2 <- spawnIO $ suffix que rdcd
            m3 <- inner rdbd wdcd
            pure $ m1 >> m2 >> m3
        where
            prefix :: Queue (IORef (Maybe d))
                        -> Duct.WriteDuct (b, d)
                        -> IO ()
            prefix que wdbd =
                withCloseQueue que $ do
                    Duct.withReadDuct rd Nothing $ \rb ->
                        Duct.withWriteDuct wdbd Nothing $ \wbd -> do
                            let recur :: MaybeT IO Void
                                recur = do
                                    b <- doRead rb
                                    dref :: IORef (Maybe d)
                                        <- lift $ newIORef Nothing
                                    let d = unsafePerformIO $ readref dref
                                    queueAdd que dref
                                    doWrite wbd (b, d)
                                    recur
                            _ <- runMaybeT recur
                            pure ()

            readref :: IORef (Maybe d) -> IO d
            readref dref = do
                r <- readIORef dref
                case r of
                    Nothing -> error "ParArrow loop: value forced early"
                    Just d  -> pure d

            suffix :: Queue (IORef (Maybe d))
                        -> Duct.ReadDuct (c, d)
                        -> IO ()
            suffix que rdcd = do
                Duct.withReadDuct rdcd Nothing $ \rcd ->
                    Duct.withWriteDuct wd Nothing $ \wc ->
                        let recur :: MaybeT IO Void
                            recur = do
                                (c, d) <- doRead rcd
                                dref <- queueGet que
                                lift $ writeIORef dref (Just d)
                                doWrite wc c
                                recur
                        in do
                            _ <- runMaybeT recur
                            pure ()

