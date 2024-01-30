{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module      : Data.Conduit.Parallel.Internal.Copier
-- Description : Code to copy values from a read duct to 1 or more write ducts.
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
module Data.Conduit.Parallel.Internal.Copier (
    copier
) where

    import           Control.Monad.Cont
    import           Data.Conduit.Parallel.Internal.Duct
    import           UnliftIO

    copier :: forall a m r .
                (MonadUnliftIO m
                , Monoid r)
                => ReadDuct a
                -> WriteDuct a
                -> [ WriteDuct a ]
                -> m r
    copier rd wd1 wds = liftIO $ finally (loop (wd1 : wds)) closeAll
        where
            loop :: [ WriteDuct a ] -> IO r
            loop [] = pure mempty
            loop ws = do
                mr :: Maybe a <- readDuct rd Nothing
                case mr of
                    Nothing -> pure mempty
                    Just r -> doWrites r ws []

            doWrites :: a
                        -> [ WriteDuct a ]
                        -> [ WriteDuct a ]
                        -> IO r
            doWrites _ []       rs = loop $ reverse rs
            doWrites r (w : ws) rs = do
                o :: Open <- writeDuct w Nothing r
                case o of
                    Closed -> doWrites r ws rs
                    Open   -> doWrites r ws (w : rs)

            closeAll :: IO ()
            closeAll = do
                _ <- closeReadDuct rd
                closeWriteDuct wd1
                mapM_ closeWriteDuct wds

