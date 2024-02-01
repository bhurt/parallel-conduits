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
    copier,
    duplicator,
    router
) where

    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.These

    doRead :: forall a .
                IO (Maybe a)
                -> (a -> IO ())
                -> IO ()
    doRead rd go = do
        ma <- rd
        case ma of
            Nothing -> pure ()
            Just a  -> go a

    doWrite1 :: forall a .
                    (a -> IO Open)
                    -> IO ()
                    -> a
                    -> IO ()
    doWrite1 wd onOpen v = do
        o <- wd v
        case o of
            Open   -> onOpen
            Closed -> pure ()

    doWrite2 :: forall a b .
                (a -> IO Open)
                -> (b -> IO Open)
                -> IO ()
                -> a
                -> b
                -> IO ()
    doWrite2 wda wdb go a b = do
        oa <- wda a
        ob <- wdb b
        if (oa == Open) && (ob == Open)
        then go
        else pure ()


    copier :: forall a .
                ReadDuct a
                -> WriteDuct a
                -> IO ()
    copier src snk = do
        withReadDuct src Nothing $ \rd ->
            withWriteDuct snk Nothing $ \wd ->
                let loop :: IO ()
                    loop = doRead rd $ doWrite1 wd loop
                in
                loop

    duplicator :: forall a .
                    ReadDuct a
                    -> WriteDuct a
                    -> WriteDuct a
                    -> IO ()
    duplicator src snk1 snk2 =
        withReadDuct src Nothing $ \rd ->
            withWriteDuct snk1 Nothing $ \wd1 ->
                withWriteDuct snk2 Nothing $ \wd2 ->
                    let loop :: IO ()
                        loop = doRead rd $ \v -> doWrite2 wd1 wd2 loop v v
                    in
                    loop

    router :: forall a b c .
                (a -> These b c)
                -> ReadDuct a
                -> WriteDuct b
                -> WriteDuct c
                -> IO ()
    router f srca snkb snkc = do
        withReadDuct srca Nothing $ \rda ->
            withWriteDuct snkb Nothing $ \wdb ->
                withWriteDuct snkc Nothing $ \wdc ->
                    let loop :: IO ()
                        loop =
                            doRead rda $ \a ->
                                case f a of
                                    This b    -> doWrite1 wdb loop b
                                    That c    -> doWrite1 wdc loop c
                                    These b c -> doWrite2 wdb wdc loop b c
                    in
                    loop
