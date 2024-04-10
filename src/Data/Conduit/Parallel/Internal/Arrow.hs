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
-- This is an internal module of the Parallel Conduits library.  You
-- almost certainly want to use "Data.Conduit.Parallel"  instead.  
-- Anything in this module not explicitly re-exported by
-- "Data.Conduit.Parallel" is for internal use only, and will change
-- or disappear without notice.  Use at your own risk.
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
    import qualified Control.Category                       as Cat
    import           Control.DeepSeq
    import qualified Control.Exception                      as Ex
    import           Control.Selective
    import           Data.Bitraversable
    import           Data.Conduit.Parallel.Internal.Control
    import           Data.Conduit.Parallel.Internal.Flip
    import           Data.Conduit.Parallel.Internal.LiftC
    import           Data.Conduit.Parallel.Internal.Type
    import           Data.Conduit.Parallel.Internal.Worker
    import qualified Data.Functor.Contravariant             as Contra
    import qualified Data.Profunctor                        as Pro
    import           System.IO.Unsafe                       (unsafePerformIO)
    import           UnliftIO


    newtype ParArrow m i o = ParArrow {
                                    getParArrow ::
                                        forall x .
                                        ReadDuct i
                                        -> WriteDuct o
                                        -> Control x m (m ())
                                }


    -- | Wrap a ParArrow to take and return a traversable data structure.
    --
    -- ![image](docs/wrapA.svg)
    --
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
                    -> Control x m (m ())
            go rdfi wdfo = do
                (rdi, wdi) :: Duct i <- newDuct
                (rdo, wdo) :: Duct o <- newDuct
                que :: Queue (f ()) <- makeQueue
                m1 <- spawnWorker $ splitter que rdfi wdi
                m2 <- spawnWorker $ fuser que rdo wdfo
                m3 <- getParArrow pa rdi wdo
                pure $ m1 >> m2 >> m3

            splitter :: Queue (f ())
                        -> ReadDuct (f i)
                        -> WriteDuct i
                        -> Worker ()
            splitter que rdfi wdi = do
                writeq :: Writer (f ()) <- openWriteQueue que
                readfi :: Reader (f i)  <- openReadDuct rdfi
                writei :: Writer i      <- openWriteDuct wdi
                let recur :: LoopM Void
                    recur = do
                        fi :: f i <- readfi
                        fu :: f () <- traverse writei fi
                        writeq fu
                        recur
                runLoopM recur

            fuser :: Queue (f ())
                        -> ReadDuct o
                        -> WriteDuct (f o)
                        -> Worker ()
            fuser que rdo wdfo = do
                readq   :: Reader (f ()) <- openReadQueue que
                reado   :: Reader o      <- openReadDuct rdo
                writefo :: Writer (f o)  <- openWriteDuct wdfo
                let recur :: LoopM Void
                    recur = do
                        fu :: f () <- readq
                        fo :: f o <- traverse (\() -> reado) fu
                        writefo fo
                        recur
                runLoopM recur

    -- | Route a bitraversable structure to either of two inner ParArrows.
    --
    -- ![image](docs/routeA.svg)
    --
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
                    -> Control x m (m ())
            go rdfi wdfo = do
                (rdi1, wdi1) :: Duct i1 <- newDuct
                (rdi2, wdi2) :: Duct i2 <- newDuct
                (rdo1, wdo1) :: Duct o1 <- newDuct
                (rdo2, wdo2) :: Duct o2 <- newDuct
                que :: Queue (f () ()) <- makeQueue
                m1 <- spawnWorker $ splitter que rdfi wdi1 wdi2
                m2 <- spawnWorker $ fuser que rdo1 rdo2 wdfo
                m3 <- getParArrow pa1 rdi1 wdo1
                m4 <- getParArrow pa2 rdi2 wdo2
                pure $ m1 >> m2 >> m3 >> m4

            splitter :: Queue (f () ())
                        -> ReadDuct (f i1 i2)
                        -> WriteDuct i1
                        -> WriteDuct i2
                        -> Worker ()
            splitter que rdfi wdi1 wdi2 = do
                writeq  :: Writer (f () ()) <- openWriteQueue que
                readfi  :: Reader (f i1 i2) <- openReadDuct rdfi
                writei1 :: Writer i1        <- openWriteDuct wdi1
                writei2 :: Writer i2        <- openWriteDuct wdi2
                let recur :: LoopM Void
                    recur = do
                        fi :: f i1 i2 <- readfi
                        fu :: f () () <- bitraverse writei1 writei2 fi
                        writeq fu
                        recur
                runLoopM recur

            fuser :: Queue (f () ())
                        -> ReadDuct o1
                        -> ReadDuct o2
                        -> WriteDuct (f o1 o2)
                        -> Worker ()
            fuser que rdo1 rdo2 wdfo = do
                readq   :: Reader (f () ()) <- openReadQueue que
                reado1  :: Reader o1        <- openReadDuct rdo1
                reado2  :: Reader o2        <- openReadDuct rdo2
                writefo :: Writer (f o1 o2) <- openWriteDuct wdfo
                let recur :: LoopM Void
                    recur = do
                        fu :: f () () <- readq
                        fo :: f o1 o2 <- bitraverse
                                            (\() -> reado1)
                                            (\() -> reado2)
                                            fu
                        writefo fo
                        recur
                runLoopM recur

    -- | Lift a Kleisli arrow into a ParArrow.
    --
    -- This creates a ParArrow that spawns a single thread, which calls
    -- the Kleisli function on every value.  Note that since `Kleisli`
    -- is itself an arrow, it can represent an arbitrary complicated
    -- pipeline itself.  But this entire pipeline will be executed in
    -- a single thread.
    liftK :: forall m i o . MonadUnliftIO m => Kleisli m i o -> ParArrow m i o
    liftK (Kleisli f) = ParArrow $ liftF f

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
                            -> Control t m (m ())
                    go rd wr = spawnWorker $ copier rd wr

        a1 . a2 = ParArrow $
                    \rd wd -> do
                        (rx, wx) :: Duct x <- newDuct
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
                        -> Control x m (m ())
                go rdi wdb = do
                    (rdi1, wdi1) :: Duct i <- newDuct
                    (rdi2, wdi2) :: Duct i <- newDuct
                    (rde, wde) :: Duct (Either a b) <- newDuct
                    (rdf, wdf) :: Duct (a -> b) <- newDuct
                    quei :: Queue i <- makeQueue
                    quee :: Queue (Either a b) <- makeQueue

                    s1 :: m () <- spawnWorker $ shim1 rdi wdi1 quei
                    m1 :: m () <- getParArrow one rdi1 wde
                    s2 :: m () <- spawnWorker $ shim2 quei rde quee wdi2
                    m2 :: m () <- getParArrow two rdi2 wdf
                    s3 :: m () <- spawnWorker $ shim3 rdf quee wdb
                    pure $ s1 >> s2 >> s3 >> m1 >> m2

                shim1 :: ReadDuct i
                            -> WriteDuct i
                            -> Queue i
                            -> Worker ()
                shim1 rdi wdi quei = do
                    writeq :: Writer i <- openWriteQueue quei
                    readi  :: Reader i <- openReadDuct rdi
                    writei :: Writer i <- openWriteDuct wdi
                    let recur :: LoopM Void
                        recur = do
                            i :: i <- readi
                            writei i
                            writeq i
                            recur
                    runLoopM recur

                shim2 :: Queue i
                            -> ReadDuct (Either a b)
                            -> Queue (Either a b)
                            -> WriteDuct i
                            -> Worker ()
                shim2 quei rde quee wdi = do
                    readqi  :: Reader i            <- openReadQueue quei
                    writeqe :: Writer (Either a b) <- openWriteQueue quee
                    reade   :: Reader (Either a b) <- openReadDuct rde
                    writei  :: Writer i            <- openWriteDuct wdi
                    let recur :: LoopM Void
                        recur = do
                            e :: Either a b <- reade
                            i :: i <- readqi
                            case e of
                                Left _  -> writei i
                                Right _ -> pure ()
                            writeqe e
                            recur
                    runLoopM recur

                shim3 :: ReadDuct (a -> b)
                            -> Queue (Either a b)
                            -> WriteDuct b
                            -> Worker ()
                shim3 rdf quee wdb = do
                    readqe :: Reader (Either a b) <- openReadQueue quee
                    readf  :: Reader (a -> b)     <- openReadDuct rdf
                    writeb :: Writer b            <- openWriteDuct wdb
                    let recur :: LoopM Void
                        recur = do
                            e :: Either a b <- readqe
                            b :: b <-
                                case e of
                                    Left a -> do
                                        f <- readf
                                        pure $ f a
                                    Right b -> pure b
                            writeb b
                            recur
                    runLoopM recur

    instance MonadUnliftIO m => ArrowLoop (ParArrow m) where
        loop :: forall b c d .
                    ParArrow m (b, d) (c, d)
                    -> ParArrow m b c
        loop inner = ParArrow $ go
            where
                go :: forall x .
                        ReadDuct b
                        -> WriteDuct c
                        -> Control x m (m ())
                go rdb wdc = do
                    (rdbd, wdbd) :: Duct (b, d) <- newDuct
                    (rdcd, wdcd) :: Duct (c, d) <- newDuct
                    que :: Queue (IORef (Maybe d)) <- makeQueue
                    m1 <- spawnWorker $ splitter que rdb wdbd
                    m2 <- spawnWorker $ fuser que rdcd wdc
                    m3 <- getParArrow inner rdbd wdcd
                    pure $ m1 >> m2 >> m3

                splitter :: Queue (IORef (Maybe d))
                            -> ReadDuct b
                            -> WriteDuct (b, d)
                            -> Worker ()
                splitter que rdb wdbd = do
                    writeq  :: Writer (IORef (Maybe d)) <- openWriteQueue que
                    readb   :: Reader b                 <- openReadDuct rdb
                    writebd :: Writer (b, d)            <- openWriteDuct wdbd
                    let recur :: LoopM Void
                        recur = do
                            b :: b <- readb
                            ref :: IORef (Maybe d)
                                <- liftIO $ newIORef Nothing
                            writeq ref
                            writebd (b, getRef ref)
                            recur
                    runLoopM recur

                getRef :: IORef (Maybe d) -> d
                getRef ref =
                    -- Hold my beer and watch this.
                    unsafePerformIO $ do
                        r <- readIORef ref
                        case r of
                            Nothing ->
                                error "ParArrow loop: value forces too soon"
                            Just d  -> pure d

                fuser :: Queue (IORef (Maybe d))
                        -> ReadDuct (c,d)
                        -> WriteDuct c
                        -> Worker ()
                fuser que rdcd wdc = do
                    readq  :: Reader (IORef (Maybe d)) <- openReadQueue que
                    readcd :: Reader (c, d)            <- openReadDuct rdcd
                    writec :: Writer c                 <- openWriteDuct wdc
                    let recur :: LoopM Void
                        recur = do
                            ref :: IORef (Maybe d) <- readq
                            (c, d) :: (c, d) <- readcd
                            liftIO $ writeIORef ref (Just d)
                            writec c
                            recur
                    runLoopM recur

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
            go :: forall x .  ReadDuct i -> WriteDuct o -> Control x m (m ())
            go rd wd =
                let wd' = contramapIO (Ex.evaluate . force) wd in
                getParArrow pa rd wd'


