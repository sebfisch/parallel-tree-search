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

module Control.Concurrent.ParallelTreeSearch ( parallelTreeSearch ) where

import Control.Monad.SearchTree
import Control.Concurrent

-- |
-- This function enumerates the results stored in a @SearchTree@ in
-- parallel. It is parameterised by the maximum number of threads to
-- use and the maximum amount of work to perform by each thread before
-- communicating the results.
--
parallelTreeSearch :: Int          -- ^ thread limit
                   -> Int          -- ^ work limit
                   -> SearchTree a -- ^ search space represented as tree
                   -> IO [a]
parallelTreeSearch tl wl t =
 do counter <- newMVar 1
    channel <- newChan
    let env = SearchEnv tl wl counter channel
    forkIO (parSearch env [] [t])
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

-- We use a queue to represent the search. Currently, we use a stack,
-- but we could also use a fifo queue to obtain breadth-first search.
--
type Queue a = [SearchTree a]

parSearch :: SearchEnv a -> [a] -> Queue a -> IO ()
parSearch env xs [] = do writeResults env xs
                         finaliseResults env
parSearch env xs q  =
 do noMoreThreads <- threadLimitReached env
    if noMoreThreads
     then let (ys,q') = search (workLimit env) xs q
           in do writeResults env ys
                 parSearch env [] q'
     else do (ys,q') <- process env [] q
             parSearch env ys q'

-- forks a new thread if the first entry of the given queue is a
-- choice.
--
process :: SearchEnv a -> [a] -> Queue a -> IO ([a], Queue a)
process _   xs []                 = return (xs,[])
process env xs (None          :q) = process env xs q
process env xs (One x         :q) = process env (x:xs) q
process env xs (t@(Choice _ _):q) = do incThreadCounter env
                                       forkIO (parSearch env xs [t])
                                       return ([],q)

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


search :: Int -> [a] -> Queue a -> ([a],Queue a)
search _ xs []               = (xs,[])
search 0 xs q                = (xs,q)
search n xs (None       : q) = search (n-1) xs q
search n xs (One x      : q) = search (n-1) (x:xs) q
search n xs (Choice s t : q) = search (n-1) xs (s:t:q)
