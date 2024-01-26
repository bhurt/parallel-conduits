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
    copier rd wd1 wds = liftIO $ finally loop closeAll
        where
            loop :: IO r
            loop = do
                mr :: Maybe a <- readDuct rd Nothing
                case mr of
                    Nothing -> pure mempty
                    Just r -> doWrites r wd1 wds

            doWrites :: a -> WriteDuct a -> [ WriteDuct a ] -> IO r
            doWrites r w1 ws = do
                o :: Open <- writeDuct w1 Nothing r
                case o of
                    Closed -> pure mempty
                    Open ->
                        case ws of
                            (x:xs) -> doWrites r x xs
                            []     -> loop

            closeAll :: IO ()
            closeAll = do
                _ <- closeReadDuct rd
                closeWriteDuct wd1
                mapM_ closeWriteDuct wds

