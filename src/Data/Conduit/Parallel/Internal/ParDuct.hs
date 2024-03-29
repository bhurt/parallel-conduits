{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.ParDuct
-- Description : Wrapper module to fit Ducts into the ParConduit library
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
-- This module wraps the various functions in 
-- "Data.Conduit.Parallel.Internal.Duct" in such a way as they work with
-- the greater ParConduit ecosystem.  This lets Ducts stay their own thing,
-- and one day possibly be spun off into their own library.
--
-- You should import this module rather than Duct itself.
--
module Data.Conduit.Parallel.Internal.ParDuct(
    Duct.Open(..),
    Duct.ReadDuct,
    Duct.WriteDuct,
    Duct.Duct,
    newDuct,
    newFullDuct,
    Duct.newClosedDuct,
    withReadDuct,
    withWriteDuct,
    addReadOpens,
    addWriteOpens,
    Duct.contramapIO
) where


    import           Control.Monad.Cont
    import           Control.Monad.Trans.Maybe
    import qualified Data.Conduit.Parallel.Internal.Duct  as Duct
    import           Data.Conduit.Parallel.Internal.Spawn
    import           Data.Conduit.Parallel.Internal.Utils

    -- | Create a new empty Duct.
    newDuct :: forall a m x . MonadIO m => Control x m (Duct.Duct a)
    newDuct = liftIO $ Duct.newDuct

    -- | Create a new duct that already holds a value.
    newFullDuct :: forall a m x . MonadIO m => a -> Control x m (Duct.Duct a)
    newFullDuct a = liftIO $ Duct.newFullDuct a

    -- | Allow reading from a ReadDuct.
    --
    -- Converts a ReadDuct into a function that reads values from that
    -- read duct.  This function uses the standard Haskell with*
    -- pattern for wrapping `Control.Exception.bracket`.  When the
    -- wrapped computation exits (either via normal value return or
    -- due to an exception), the open count of the read duct is
    -- decremented.  If it drops to zero, then the read is closed.
    -- 
    withReadDuct :: forall a .  Duct.ReadDuct a -> Worker (Reader a)
    withReadDuct rd = do
        r :: IO (Maybe a) <- ContT $ Duct.withReadDuct rd Nothing
        pure $ MaybeT r

    -- | Allow writing to a WriteDuct.
    --
    -- Converts a WriteDuct into a function that writes values to that
    -- write duct.  This function uses the standard Haskell with*
    -- pattern for wrapping `Control.Exception.bracket`.  When the
    -- wrapped computation exits (either via normal value return or
    -- due to an exception), the open count of the write duct is
    -- decremented.  If it drops to zero, then the duct is closed.
    -- 
    withWriteDuct :: forall a . Duct.WriteDuct a -> Worker (Writer a)
    withWriteDuct wd = do
        wio :: (a -> IO Duct.Open) <- ContT $ Duct.withWriteDuct wd Nothing
        let wm :: a -> RecurM ()
            wm a = MaybeT $ do
                open <- wio a
                case open of
                    Duct.Open   -> pure $ Just ()
                    Duct.Closed -> pure Nothing
        pure wm

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
