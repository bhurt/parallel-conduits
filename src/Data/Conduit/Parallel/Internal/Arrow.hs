{-# LANGUAGE InstanceSigs        #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Arrow
-- Description : ParArrow: parallel arrows.
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
module Data.Conduit.Parallel.Internal.Arrow (
    ParArrow(..),
    wrapA,
    routeA,
    toParConduit,
    liftK,
    forceA
) where

    import           Control.Arrow
    import qualified Control.Category                      as Cat
    import           Control.DeepSeq
    import qualified Control.Exception                     as Ex
    import           Control.Monad.Cont                    (ContT, lift)
    import           Control.Monad.Trans.Maybe
    import           Control.Selective
    import           Data.Bitraversable
    import           Data.Conduit.Parallel.Internal.Copier (copier)
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Conduit.Parallel.Internal.Flip
    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Conduit.Parallel.Internal.Type
    import           Data.Conduit.Parallel.Internal.Utils
    import qualified Data.Functor.Contravariant            as Contra
    import qualified Data.Profunctor                       as Pro
    import           Data.Void
    import           System.IO.Unsafe                      (unsafePerformIO)
    import           UnliftIO


    newtype ParArrow m i o = ParArrow {
                                    getParArrow ::
                                        forall x .
                                        ReadDuct i
                                        -> WriteDuct o
                                        -> ContT x m (m ())
                                }


    wrapA :: forall m i o f .
                (MonadUnliftIO m
                , Traversable f)
                => ParArrow m i o
                -> ParArrow m (f i) (f o)
    wrapA pa = ParArrow go
        where
            go :: forall x .
                    ReadDuct (f i)
                    -> WriteDuct (f o)
                    -> ContT x m (m ())
            go rdfi wdfo = do
                (rdi, wdi) <- liftIO newDuct
                (rdo, wdo) <- liftIO newDuct
                que :: Queue (f ()) <- makeQueue
                m1 <- spawnIO $ splitter que rdfi wdi
                m2 <- spawnIO $ fuser que rdo wdfo
                m3 <- getParArrow pa rdi wdo
                pure $ m1 >> m2 >> m3

            splitter :: Queue (f ()) -> ReadDuct (f i) -> WriteDuct i -> IO ()
            splitter que rdfi wdi = do
                withCloseQueue que $
                    withReadDuct rdfi Nothing $ \rfi ->
                        withWriteDuct wdi Nothing $ \wi ->
                            let recur :: MaybeT IO Void
                                recur = do
                                    fi :: f i <- readM rfi
                                    fu :: f () <- traverse (writeM wi) fi
                                    queueAdd que fu
                                    recur
                            in
                            runM recur

            fuser :: Queue (f ()) -> ReadDuct o -> WriteDuct (f o) -> IO ()
            fuser que rdo wdfo = do
                withReadDuct rdo Nothing $ \ro ->
                    withWriteDuct wdfo Nothing $ \wfo ->
                        let recur :: MaybeT IO Void
                            recur = do
                                fu :: f () <- queueGet que
                                fo :: f o <- traverse (\() -> readM ro) fu
                                writeM wfo fo
                                recur
                        in
                        runM recur

    routeA :: forall m i1 i2 o1 o2 f .
                (MonadUnliftIO m
                , Bitraversable f)
                => ParArrow m i1 o1
                -> ParArrow m i2 o2
                -> ParArrow m (f i1 i2) (f o1 o2)
    routeA pa1 pa2 = ParArrow go
        where
            go :: forall x .
                    ReadDuct (f i1 i2)
                    -> WriteDuct (f o1 o2)
                    -> ContT x m (m ())
            go rdfi wdfo = do
                (rdi1, wdi1) <- liftIO newDuct
                (rdi2, wdi2) <- liftIO newDuct
                (rdo1, wdo1) <- liftIO newDuct
                (rdo2, wdo2) <- liftIO newDuct
                que :: Queue (f () ()) <- makeQueue
                m1 <- spawnIO $ splitter que rdfi wdi1 wdi2
                m2 <- spawnIO $ fuser que rdo1 rdo2 wdfo
                m3 <- getParArrow pa1 rdi1 wdo1
                m4 <- getParArrow pa2 rdi2 wdo2
                pure $ m1 >> m2 >> m3 >> m4

            splitter :: Queue (f () ())
                        -> ReadDuct (f i1 i2)
                        -> WriteDuct i1
                        -> WriteDuct i2
                        -> IO ()
            splitter que rdfi wdi1 wdi2 = do
                withCloseQueue que $
                    withReadDuct rdfi Nothing $ \rfi ->
                        withWriteDuct wdi1 Nothing $ \wi1 ->
                            withWriteDuct wdi2 Nothing $ \wi2 ->
                                let recur :: MaybeT IO Void
                                    recur = do
                                        fi :: f i1 i2 <- readM rfi
                                        fu :: f () () <- bitraverse
                                                            (writeM wi1)
                                                            (writeM wi2)
                                                            fi
                                        queueAdd que fu
                                        recur
                                in
                                runM recur

            fuser :: Queue (f () ())
                        -> ReadDuct o1
                        -> ReadDuct o2
                        -> WriteDuct (f o1 o2)
                        -> IO ()
            fuser que rdo1 rdo2 wdfo = do
                withReadDuct rdo1 Nothing $ \ro1 ->
                    withReadDuct rdo2 Nothing $ \ro2 ->
                        withWriteDuct wdfo Nothing $ \wfo ->
                            let recur :: MaybeT IO Void
                                recur = do
                                    fu :: f () () <- queueGet que
                                    fo :: f o1 o2 <- bitraverse
                                                        (\() -> readM ro1)
                                                        (\() -> readM ro2)
                                                        fu
                                    writeM wfo fo
                                    recur
                            in
                            runM recur

    -- | Lift a Kleisli arrow into a ParArrow.
    --
    -- This creates a ParArrow that spawns a single thread, which calls
    -- the Kleisli function on every value.  Note that since `Kleisli`
    -- is itself an arrow, it can represent an arbitrary complicated
    -- pipeline itself.  But this entire pipeline will be executed in
    -- a single thread.
    liftK :: forall m i o . MonadUnliftIO m => Kleisli m i o -> ParArrow m i o
    liftK (Kleisli f) = ParArrow go
        where
            go :: forall x .  ReadDuct i -> WriteDuct o -> ContT x m (m ())
            go rdi wdo = spawn $ worker rdi wdo

            worker :: ReadDuct i -> WriteDuct o -> m ()
            worker rdi wdo = do
                withReadDuct rdi Nothing $ \ri ->
                    withWriteDuct wdo Nothing $ \wo ->
                        let recur :: MaybeT m Void
                            recur = do
                                i <- readM ri
                                o <- lift $ f i
                                writeM wo o
                                recur
                        in
                        runM recur


    instance Functor (ParArrow m i) where
        fmap f c = ParArrow $
                    \r w -> getParArrow c r (Contra.contramap f w)

    instance Pro.Profunctor (ParArrow m) where
        dimap f g c = ParArrow $
                        \rd wd ->
                            getParArrow c
                                (fmap f rd)
                                (Contra.contramap g wd)
        lmap f c = ParArrow $
                    \rd wd ->
                        getParArrow c
                            (fmap f rd)
                            wd

        rmap g c = ParArrow $
                    \rd wd ->
                        getParArrow c
                            rd
                            (Contra.contramap g wd)

    instance (MonadUnliftIO m) => Cat.Category (ParArrow m) where
        id :: forall a . ParArrow m a a
        id = ParArrow go
                where
                    go :: ReadDuct a
                            -> WriteDuct a
                            -> ContT t m (m ())
                    go rd wr = spawnIO $ copier rd wr

        a1 . a2 = ParArrow $
                    \rd wd -> do
                        (rx, wx) <- liftIO $ newDuct
                        r1 <- getParArrow a2 rd wx
                        r2 <- getParArrow a1 rx wd
                        pure $ r1 >> r2

    instance MonadUnliftIO m => Arrow (ParArrow m) where
        arr f = liftK (Kleisli (pure . f))

        first :: forall b c d .
                    ParArrow m b c
                    -> ParArrow m (b, d) (c, d)
        first pa = Pro.dimap Flip unFlip $ wrapA pa

        second :: forall b c d .
                    ParArrow m b c
                    -> ParArrow m (d, b) (d, c)
        second pa = wrapA pa

        (***) :: forall b c b' c' .
                    ParArrow m b c
                    -> ParArrow m b' c'
                    -> ParArrow m (b, b') (c, c')
        p1 *** p2 = routeA p1 p2

        (&&&) :: forall b c c' .
                    ParArrow m b c
                    -> ParArrow m b c'
                    -> ParArrow m b (c, c')
        p1 &&& p2 = Pro.lmap (\b -> (b, b)) $ routeA p1 p2

    instance MonadUnliftIO m => ArrowChoice (ParArrow m) where

        left :: forall b c d .
                    ParArrow m b c
                    -> ParArrow m (Either b d) (Either c d)
        left pa = Pro.dimap Flip unFlip $ wrapA pa

        right :: forall b c d .
                    ParArrow m b c
                    -> ParArrow m (Either d b) (Either d c)
        right pa = wrapA pa

        (+++) :: forall b c b' c' .
                    ParArrow m b c
                    -> ParArrow m b' c'
                    -> ParArrow m (Either b b') (Either c c')
        pa1 +++ pa2 = routeA pa1 pa2


        (|||) :: forall b c d .
                    ParArrow m b d
                    -> ParArrow m c d
                    -> ParArrow m (Either b c) d
        pa1 ||| pa2 = fmap go $ routeA pa1 pa2
            where
                go :: Either d d -> d
                go (Left d)  = d
                go (Right d) = d

    instance MonadUnliftIO m => Applicative (ParArrow m a) where
        pure a = liftK . Kleisli $ \_ -> pure a
        pa1 <*> pa2 = Pro.dimap (\i -> (i, i)) (\(f, o) -> f o) $ routeA pa1 pa2

    instance MonadUnliftIO m => Selective (ParArrow m i) where
        select :: forall a b .
                    ParArrow m i (Either a b)
                    -> ParArrow m i (a -> b)
                    -> ParArrow m i b
        select one two = ParArrow go
            where
                go :: forall x .
                        ReadDuct i
                        -> WriteDuct b
                        -> ContT x m (m ())
                go rdi wdb = do
                    (rdi1, wdi1) :: Duct i <- liftIO newDuct
                    (rdi2, wdi2) :: Duct i <- liftIO newDuct
                    (rde, wde) :: Duct (Either a b) <- liftIO newDuct
                    (rdf, wdf) :: Duct (a -> b) <- liftIO newDuct
                    quei :: Queue i <- makeQueue
                    quee :: Queue (Either a b) <- makeQueue

                    s1 :: m () <- spawnIO $ shim1 rdi wdi1 quei
                    m1 :: m () <- getParArrow one rdi1 wde
                    s2 :: m () <- spawnIO $ shim2 quei rde quee wdi2
                    m2 :: m () <- getParArrow two rdi2 wdf
                    s3 :: m () <- spawnIO $ shim3 rdf quee wdb
                    pure $ s1 >> s2 >> s3 >> m1 >> m2

                shim1 :: ReadDuct i
                            -> WriteDuct i
                            -> Queue i
                            -> IO ()
                shim1 rdi wdi quei =
                    withCloseQueue quei $
                        withReadDuct rdi Nothing $ \ri ->
                            withWriteDuct wdi Nothing $ \wi ->
                                let recur :: MaybeT IO Void
                                    recur = do
                                        i :: i <- readM ri
                                        writeM wi i
                                        queueAdd quei i
                                        recur
                                in
                                runM recur

                shim2 :: Queue i
                            -> ReadDuct (Either a b)
                            -> Queue (Either a b)
                            -> WriteDuct i
                            -> IO ()
                shim2 quei rde quee wdi =
                    withCloseQueue quee $
                        withReadDuct rde Nothing $ \re ->
                            withWriteDuct wdi Nothing $ \wi ->
                                let recur :: MaybeT IO Void
                                    recur = do
                                        e :: Either a b <- readM re
                                        i :: i <- queueGet quei
                                        case e of
                                            Left _  -> writeM wi i
                                            Right _ -> pure ()
                                        queueAdd quee e
                                        recur
                                in
                                runM recur

                shim3 :: ReadDuct (a -> b)
                            -> Queue (Either a b)
                            -> WriteDuct b
                            -> IO ()
                shim3 rdf quee wdb =
                    withReadDuct rdf Nothing $ \rf ->
                        withWriteDuct wdb Nothing $ \wb ->
                            let recur :: MaybeT IO Void
                                recur = do
                                    e :: Either a b <- queueGet quee
                                    b :: b <-
                                        case e of
                                            Left a -> do
                                                f <- readM rf
                                                pure $ f a
                                            Right b -> pure b
                                    writeM wb b
                                    recur
                            in
                            runM recur

    instance MonadUnliftIO m => ArrowLoop (ParArrow m) where
        loop :: forall b c d .
                    ParArrow m (b, d) (c, d)
                    -> ParArrow m b c
        loop inner = ParArrow $ go
            where
                go :: forall x .
                        ReadDuct b
                        -> WriteDuct c
                        -> ContT x m (m ())
                go rdb wdc = do
                    (rdbd, wdbd) <- liftIO newDuct
                    (rdcd, wdcd) <- liftIO newDuct
                    que :: Queue (IORef (Maybe d)) <- makeQueue
                    m1 <- spawnIO $ splitter que rdb wdbd
                    m2 <- spawnIO $ fuser que rdcd wdc
                    m3 <- getParArrow inner rdbd wdcd
                    pure $ m1 >> m2 >> m3

                splitter :: Queue (IORef (Maybe d))
                            -> ReadDuct b
                            -> WriteDuct (b, d)
                            -> IO ()
                splitter que rdb wdbd = do
                    withCloseQueue que $
                        withReadDuct rdb Nothing $ \rb ->
                            withWriteDuct wdbd Nothing $ \wbd ->
                                let recur :: MaybeT IO Void
                                    recur = do
                                        b <- readM rb
                                        ref :: IORef (Maybe d)
                                            <- liftIO $ newIORef Nothing
                                        queueAdd que ref
                                        writeM wbd (b, getRef ref)
                                        recur
                                in
                                runM recur

                getRef :: IORef (Maybe d) -> d
                getRef ref =
                    -- Hold my beer and watch this.
                    unsafePerformIO $ do
                        r <- readIORef ref
                        case r of
                            Nothing -> error "ParArrow loop: value forces too soon"
                            Just d  -> pure d

                fuser :: Queue (IORef (Maybe d))
                        -> ReadDuct (c,d)
                        -> WriteDuct c
                        -> IO ()
                fuser que rdcd wdc = do
                    withReadDuct rdcd Nothing $ \rcd ->
                        withWriteDuct wdc Nothing $ \wc ->
                            let recur :: MaybeT IO Void
                                recur = do
                                    ref <- queueGet que
                                    (c, d) <- readM rcd
                                    liftIO $ writeIORef ref (Just d)
                                    writeM wc c
                                    recur
                            in
                            runM recur

    -- | Convert a ParArrow to a ParConduit
    --
    -- As this is just unwrapping one newtype and wrapping a second
    -- of newtype, it should be free.
    --
    toParConduit :: forall m i o . ParArrow m i o -> ParConduit m () i o
    toParConduit a = ParConduit (getParArrow a)
        -- Note: as tempting as defining this function as:
        --      toParConduit = ParConduit . getParArrow
        -- This does not work, for reasons I am unclear about but have
        -- to do with weirdness around higher ranked types.

    -- | Force the outputs of a ParArrow into normal form.
    forceA :: forall m i o .
                NFData o
                => ParArrow m i o
                -> ParArrow m i o
    forceA pa = ParArrow go
        where
            go :: forall x .  ReadDuct i -> WriteDuct o -> ContT x m (m ())
            go rd wd =
                let wd' = contramapIO (Ex.evaluate . force) wd in
                getParArrow pa rd wd'


