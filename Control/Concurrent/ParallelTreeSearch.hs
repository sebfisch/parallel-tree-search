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

module Control.Concurrent.ParallelTreeSearch ( 

  SearchQueue(..), SearchView(..), LIFO(..), FIFO(..), 

  parallelTreeSearch

 ) where

import Control.Monad.SearchTree
import Control.Concurrent
import qualified Data.Sequence as Seq

-- |
-- Search queues store multiple search trees.
-- 
class SearchQueue q
 where
  -- | Constructs an empty search queue.
  emptyQ :: q a

  -- | Adds a search tree to asearch queue.
  addQ   :: SearchTree a -> q a -> q a

  -- | creates a view on a search queue for pattern matching.
  viewQ  :: q a -> SearchView q a

-- |
-- Checks whether the given search queue is empty.
-- 
isEmptyQ :: SearchQueue q => q a -> Bool
{-# SPECIALISE INLINE isEmptyQ :: LIFO a -> Bool #-}
{-# SPECIALISE INLINE isEmptyQ :: FIFO a -> Bool #-}
isEmptyQ q = case viewQ q of EmptyQ -> True; _ -> False

-- |
-- A @SearchView@ is used for pattern matching a search queue.
-- 
data SearchView q a = EmptyQ | SearchTree a :~ q a

-- | 
-- LIFO search queues can be used to implement parallel depth-first
-- search.
-- 
newtype LIFO a = LIFO [SearchTree a]

instance SearchQueue LIFO
 where
  {-# SPECIALISE instance SearchQueue LIFO #-}

  emptyQ              = LIFO []

  addQ t (LIFO q)     = LIFO (t:q)

  viewQ (LIFO [])     = EmptyQ
  viewQ (LIFO (x:xs)) = x :~ LIFO xs

-- | 
-- FIFO search queues can be used to implement parallel breadth-first
-- search.
-- 
newtype FIFO a = FIFO (Seq.Seq (SearchTree a))

instance SearchQueue FIFO
 where 
  {-# SPECIALISE instance SearchQueue FIFO #-}

  emptyQ          = FIFO Seq.empty

  addQ t (FIFO q) = FIFO (q Seq.|> t)

  viewQ (FIFO q)  = case Seq.viewl q of
                      Seq.EmptyL  -> EmptyQ
                      x Seq.:< xs -> x :~ FIFO xs

-- |
-- This function enumerates the results stored in the queue of
-- @SearchTree@s in parallel. It is parameterised by the maximum
-- number of threads to use and the maximum amount of work to perform
-- by each thread before communicating the results.
-- 
parallelTreeSearch :: SearchQueue q
                   => Int  -- ^ thread limit
                   -> Int  -- ^ work limit
                   -> q a  -- ^ queue with search trees
                   -> IO [a]
parallelTreeSearch tl wl q =
 do counter <- newMVar 1
    channel <- newChan
    let env = SearchEnv tl wl counter channel
    forkIO (parSearch env [] q)
    xs <- getChanContents channel
    return (concNonEmpty xs)

-- like concat, but stops on empty list.
--
concNonEmpty :: [[a]] -> [a]
concNonEmpty []       = []
concNonEmpty ([]:_)   = []
concNonEmpty (xs:xss) = xs ++ concNonEmpty xss

-- Environment passed to the parallel search algorithm.
--
data SearchEnv a = SearchEnv { threadLimit   :: Int
                             , workLimit     :: Int
                             , threadCounter :: MVar Int
                             , results       :: Chan [a] }


parSearch :: SearchQueue q => SearchEnv a -> [a] -> q a -> IO ()
parSearch env xs q 
  | isEmptyQ q = do writeResults env xs
                    finaliseResults env
  | otherwise  = do noMoreThreads <- threadLimitReached env
                    if noMoreThreads
                     then let (ys,q') = search (workLimit env) xs (viewQ q)
                           in do writeResults env ys
                                 parSearch env [] q'
                     else do (ys,q') <- process env [] (viewQ q)
                             parSearch env ys q'

-- forks a new thread for the first entry of the given queue that is a
-- choice.
--
process :: SearchQueue q
        => SearchEnv a -> [a] -> SearchView q a -> IO ([a], q a)
process _   xs EmptyQ            = return (xs,emptyQ)
process env xs (None       :~ q) = process env xs (viewQ q)
process env xs (One x      :~ q) = process env (x:xs) (viewQ q)
process env xs (Choice s t :~ q) =
 do incThreadCounter env
    forkIO (parSearch env xs (addQ s (emptyQ `withTypeOf` q)))
    return ([], addQ t q)

withTypeOf :: a -> a -> a
withTypeOf = const

-- auxiliary functions

writeResults :: SearchEnv a -> [a] -> IO ()
writeResults _   [] = return ()
writeResults env xs = writeChan (results env) xs

incThreadCounter :: SearchEnv a -> IO ()
incThreadCounter env = modifyMVar_ (threadCounter env) (return.(+1))

threadLimitReached :: SearchEnv a -> IO Bool
threadLimitReached env = do count <- readMVar (threadCounter env)
                            return (count >= threadLimit env)

finaliseResults :: SearchEnv a -> IO ()
finaliseResults env = do count <- takeMVar (threadCounter env)
                         if count <= 1
                          then writeChan (results env) []
                          else putMVar (threadCounter env) (count-1)

search :: SearchQueue q => Int -> [a] -> SearchView q a -> ([a],q a)
search _ xs EmptyQ            = (xs,emptyQ)
search 0 xs (t          :~ q) = (xs,addQ t q)
search n xs (None       :~ q) = search (n-1) xs     (viewQ q)
search n xs (One x      :~ q) = search (n-1) (x:xs) (viewQ q)
search n xs (Choice s t :~ q) = search (n-1) xs     (viewQ (addQ s (addQ t q)))

