{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Control
-- Description : Control thread utilities.
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
-- = Purpose
--
-- This is a set of utilities and wrappers useful for writing code that
-- runs in the control thread.
--
-- Like the "Data.Conduit.Parallel.Internal.Worker" module, it includes
-- wrappers for the "Data.Conduit.Parallel.Internal.Duct" module,
-- specifically wrappers for the Duct functions called from the main
-- control thread.
--
module Data.Conduit.Parallel.Internal.Control(
    -- * Control Thread Type
    Control,

    -- * Spawning Threads
    spawnClient,

    -- * Ducts
    Duct.ReadDuct,
    Duct.WriteDuct,
    Duct.Duct,
    newDuct,
    newFullDuct,
    Duct.newClosedDuct,
    addReadOpens,
    addWriteOpens,
    Duct.contramapIO

) where

    import           Control.Monad.Cont
    import           Control.Monad.IO.Unlift
    import qualified Data.Conduit.Parallel.Internal.Duct   as Duct
    import qualified UnliftIO.Async                        as Async

    -- | The main control thread type.
    --
    -- There is one control thread, which only creates ducts and spawns
    -- worker and client threads.  All real work should be done in either
    -- a worker or client thread.  The control thread is executed by
    -- calling `Data.Conduit.Parallel.Internal.Run.runParConduit`.
    type Control r m a = ContT r m a

    -- | Spawn a client thread.
    --
    -- A client thread executes client-supplied code, and thus executes
    -- in the client-supplied monad.
    --
    -- The canonical users of this function are in
    -- "Data.Conduit.Parallel.Internal.LiftC".
    --
    spawnClient :: forall m r x .
                MonadUnliftIO m
                => m r
                -> Control x m (m r)
    spawnClient client = do
        asy <- ContT $ Async.withAsync client
        lift $ Async.link asy
        pure $ Async.wait asy

    -- | Create a new empty Duct.
    newDuct :: forall a m x . MonadIO m => Control x m (Duct.Duct a)
    newDuct = liftIO $ Duct.newDuct

    -- | Create a new duct that already holds a value.
    newFullDuct :: forall a m x . MonadIO m => a -> Control x m (Duct.Duct a)
    newFullDuct a = liftIO $ Duct.newFullDuct a


    -- | Add opens to a read duct.
    --
    -- Read ducts have an open count associated with them.  When multiple
    -- threads are expected to share a read duct (for example, in
    -- `Data.Conduit.Parallel.Internal.Parallel.parallel`), you can add
    -- to the count of opens expected. This way, the last open to exit
    -- closes the read duct, not the first.
    --
    -- Note that read ducts start with an open count of 1.  So, if you
    -- are going to have N threads use the read duct, you need to
    -- add N-1 extra opens.
    --
    addReadOpens :: forall a m x .
                    MonadIO m
                    => Duct.ReadDuct a
                    -> Int
                    -> Control x m ()
    addReadOpens rd n = liftIO $ Duct.addReadOpens rd n

    -- | Add opens to a write duct.
    --
    -- Write ducts have an open count associated with them.  When multiple
    -- threads are expected to share a write duct (for example, in
    -- `Data.Conduit.parallel`), you can add to the count of opens expected.
    -- This way, the last open to exit closes the write duct, not the first.
    --
    -- Note that write ducts start with an open count of 1.  So, if you
    -- are going to have N threads use the write duct, you need to
    -- add N-1 extra opens.
    --
    addWriteOpens :: forall a m x .
                        MonadIO m
                        => Duct.WriteDuct a
                        -> Int
                        -> Control x m ()
    addWriteOpens wd n = liftIO $ Duct.addWriteOpens wd n

