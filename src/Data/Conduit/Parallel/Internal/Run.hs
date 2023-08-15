{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Run
-- Description : Running parallel conduits
-- Copyright   : (c) Brian Hurt, 2023
-- License     : BSD 3-clause
-- Maintainer  : bhurt42@gmail.com
-- Stability   : experimental
--
-- = Warning
--
-- This is an internal module of the Parallel Conduits.  You almost
-- certainly want to use [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- instead.  Anything in this module not explicitly re-exported 
-- by [Data.Conduit.Parallel](Data-Conduit-Parallel.html)
-- is for internal use only, and will change or disappear without
-- notice.  Use at your own risk.
--
module Data.Conduit.Parallel.Internal.Run (
    runParConduit
) where

    import qualified Control.Monad.Cont                  as Cont
    import           Control.Monad.IO.Class              (MonadIO (..))
    import qualified Data.Conduit.Parallel.Internal.Duct as Duct
    import           Data.Conduit.Parallel.Internal.Type (ParConduit (..))
    import           Data.Functor.Contravariant          (contramap)
    import           Data.Void                           (Void, absurd)

    runParConduit :: forall m r . MonadIO m => ParConduit m r () Void -> m r
    runParConduit pc = do
        (rd, wd) <- liftIO $ Duct.newClosedDuct
        let wd' = contramap absurd wd
        Cont.runContT (getParConduit pc rd wd') id


