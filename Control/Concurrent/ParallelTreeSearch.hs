-- |
-- Module      : Control.Concurrent.ParallelTreeSearch
-- Copyright   : Fabian Reck, Sebastian Fischer
-- License     : PublicDomain
-- 
-- Maintainer  : Sebastian Fischer (sebf@informatik.uni-kiel.de)
-- Stability   : experimental
-- Portability : portable
-- 
-- This Haskell library provides an implementation of parallel search
-- based on the search tree provided by the package tree-monad.
-- 
module Control.Concurrent.ParallelTreeSearch ( parallelTreeSearch ) where

import Control.Monad
import Control.Monad.SearchTree

import Control.Concurrent

-- | Enumerate the leaves of a @SearchTree@ in parallel.
parallelTreeSearch :: Int           -- ^ number of threads to use
                   -> SearchTree a  -- ^ tree to search
                   -> IO [a]        -- ^ lazy list of leaves
parallelTreeSearch threadCount tree =
 do ctr <- newMVar 1
    res <- newChan
    queue <- newChan
    writeChan queue tree
    sequence (replicate threadCount (forkIO (search ctr res queue)))
    liftM (foldr (\mx xs -> maybe [] (:xs) mx) []) (getChanContents res)

search :: MVar Int -> Chan (Maybe a) -> Chan (SearchTree a) -> IO ()
search ctr res queue = process =<< readChan queue
 where
  process None         = finished
  process (One x)      = do writeChan res (Just x); finished
  process (Choice l r) = do modifyMVar_ ctr (return.succ)
                            writeChan queue l
                            writeChan queue r
                            search ctr res queue

  finished = do count <- modifyMVar ctr ((\n -> return (n,n)).pred)
                if count == 0 then writeChan res Nothing
                 else search ctr res queue


