{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE FlexibleContexts   #-}
{-# LANGUAGE LambdaCase         #-}
{-# LANGUAGE RecordWildCards    #-}
{-# LANGUAGE StandaloneDeriving #-}
module Control.Monad.Free.Parallel where

import           Control.Monad.Free                (Free (Free, Pure))
import qualified Control.Monad.Free                as Free
import           Control.Monad.State               (State, evalState, get, put)
import           Data.Array.Accelerate             (Z (..), (:.) (..))
import qualified Data.Array.Accelerate             as Acc
import qualified Data.Array.Accelerate.LLVM.Native as Acc
import qualified Data.Foldable                     as F
import           GHC.Generics                      (Generic)

-- $setup
-- >>> :set -XDeriveFunctor
-- >>> :set -XDeriveFoldable
-- >>> :set -XDeriveGeneric
-- >>> :set -XDeriveAnyClass
-- >>> :set -XFlexibleInstance
-- >>> import GHC.Generics (Generic)
-- >>> data Exp a = Lit a | a :+: a deriving (Eq, Show, Functor, Foldable, Generic, Acc.Elt)
-- >>> instance Num (Free Exp a) where fromInteger = Free . Lit . fromInteger; x + y = Free (x :+: y)
-- >>> sampleExp = (1 + pure 'x') + (2 + (3 + pure 'y')) :: Free Exp Char

-- | A node of the data parallel representation of a free monad.
data Node f a
  = Var a     -- ^ A variable (leaf).
  | Op (f ()) -- ^ An inner node (with all subtrees removed).
  deriving (Generic)

deriving instance (Acc.Elt a, Acc.Elt (f ())) => Acc.Elt (Node f a)
deriving instance (Eq a, Eq (f ())) => Eq (Node f a)
deriving instance (Show a, Show (f ())) => Show (Node f a)

type NodeCoords = Acc.Matrix Int

-- | Data parallel representation of a free monad.
data FreeP f a = FreeP
  { maxDepth   :: Int
    -- ^ Maximum depth in the tree.
  , depths     :: Acc.Acc (Acc.Vector Int)
    -- ^ The depth vector, specifying depth in the tree for each node.
  , nodeCoords :: Acc.Acc NodeCoords
    -- ^ Node coordinates, for tree manipulations.
  , nodes      :: Acc.Acc (Acc.Vector (Node f a))
    -- ^ The nodes vector, containing tags and attached values of nodes.
  }

deriving instance (Acc.Elt a, Acc.Elt (f ())) => Show (FreeP f a)

-- | Convert from a recursive @'Free' f a@ representation (on CPU)
-- to a prepared parallel representation (e.g. on GPU),
-- with the maximum depth in the tree automatically figured out.
--
-- Use 'ppFreeP' to pretty-print a @'FreeP' f a@ value:
--
-- >>> putStr $ ppFreeP (fromFree' sampleExp)
-- 0 	   [ 1, 0, 0, 	 Op (() :+: ())
-- 1 	     1, 1, 0, 	 Op (() :+: ())
-- 2 	     1, 1, 1, 	 Op (Lit 1)
-- 2 	     1, 1, 2, 	 Var 'x'
-- 1 	     1, 2, 0, 	 Op (() :+: ())
-- 2 	     1, 2, 3, 	 Op (Lit 2)
-- 2 	     1, 2, 4, 	 Op (() :+: ())
-- 3 	     1, 2, 4, 	 Op (Lit 3)
-- 3 	     1, 2, 4] 	 Var 'y'
fromFree' :: (Functor f, Foldable f, Acc.Elt a, Acc.Elt (f ())) => Free f a -> FreeP f a
fromFree' t = do
  FreeP{..}
  where
    (depthList, nodeList) = unzip (collectNodesWithDepth t)
    maxDepth = maximum depthList
    n = length depthList
    depths = Acc.use (Acc.fromList (Z :. n) depthList)
    nodes  = Acc.use (Acc.fromList (Z :. n) nodeList)
    nodeCoords = depthsToNodeCoords maxDepth n depths

-- | Convert from a recursive @'Free' f a@ representation (on CPU)
-- to a prepared parallel representation (e.g. on GPU),
-- given an upper bound for the maximum depth in the tree.
fromFree :: (Functor f, Foldable f, Acc.Elt a, Acc.Elt (f ())) => Int -> Free f a -> FreeP f a
fromFree maxDepth t = do
  FreeP{..}
  where
    (depthList, nodeList) = unzip (collectNodesWithDepth t)
    n = length depthList
    depths = Acc.use (Acc.fromList (Z :. n) depthList)
    nodes  = Acc.use (Acc.fromList (Z :. n) nodeList)
    nodeCoords = depthsToNodeCoords maxDepth n depths

-- | Convert from a prepared parallel representation (e.g. on GPU)
-- to a recursive @'Free' f a@ representation (back on CPU).
toFree :: (Acc.Elt a, Acc.Elt (f ()), Traversable f) => FreeP f a -> Free f a
toFree FreeP{..} = collectInOrder (Acc.toList (Acc.run nodes))

-- | Build a tree from a list of nodes (sorted according to in-order traversal of the tree).
--
-- >>> collectInOrder [Op (Just ()), Var 'x'] :: Free Maybe Char
-- Free (Just (Pure 'x'))
collectInOrder :: (Acc.Elt a, Acc.Elt (f ()), Traversable f) => [Node f a] -> Free f a
collectInOrder = evalState build
  where
    fetch = do
      get >>= \case
        [] -> error "not enough Node values"
        node : moreNodes -> do
          put moreNodes
          return node

    build = do
      fetch >>= \case
        Var x -> return (Pure x)
        Op f  -> Free <$> traverse (const build) f

-- | Compute node coordinates from depth vector.
depthsToNodeCoords
  :: Int                      -- ^ Maximum allowed depth.
  -> Int                      -- ^ Number of elements (length of the depth vector).
  -> Acc.Acc (Acc.Vector Int) -- ^ Depth vector (prepared on GPU).
  -> Acc.Acc NodeCoords       -- ^ Node coordinates (prepared on GPU).
depthsToNodeCoords maxDepth n depthVector = do
  let dims = Acc.I2 (Acc.constant maxDepth) (Acc.constant n)
      depthMatrix = Acc.generate dims $ \(Acc.I2 i j) ->
        Acc.boolToInt ((depthVector Acc.! (Acc.I1 j)) Acc.== i)
      scanMatrix = Acc.transpose (Acc.scanl1 (+) depthMatrix)
  Acc.imap (\(Acc.I2 i j) x -> x * Acc.boolToInt (j Acc.<= depthVector Acc.! (Acc.I1 i))) scanMatrix

-- | Collect nodes of the tree.
--
-- >>> collectNodes sampleExp
-- [Op (() :+: ()),Op (() :+: ()),Op (Lit 1),Var 'x',Op (() :+: ()),Op (Lit 2),Op (() :+: ()),Op (Lit 3),Var 'y']
collectNodes :: (Functor f, Foldable f) => Free f a -> [Node f a]
collectNodes = Free.iter f . fmap (pure . Var)
  where
    f t = (Op (() <$ t)) : F.fold t

-- | Collect nodes together with their corresponding depth in the tree.
--
-- FIXME: not very efficient
--
-- >>> putStr $ ppNodesWithDepth (collectNodesWithDepth sampleExp)
-- Op (() :+: ())
-- ├─ Op (() :+: ())
-- │  ├─ Op (Lit 1)
-- │  └─ Var 'x'
-- └─ Op (() :+: ())
--    ├─ Op (Lit 2)
--    └─ Op (() :+: ())
--       ├─ Op (Lit 3)
--       └─ Var 'y'
collectNodesWithDepth :: (Functor f, Foldable f) => Free f a -> [(Int, Node f a)]
collectNodesWithDepth = Free.iter f . fmap (\x -> [(0, Var x)])
  where
    f t = (0, Op (() <$ t)) : F.foldMap (map inc) t
    inc (d, x) = (d + 1, x)

-- | Pretty-print a @'FreeP' f a@ value.
--
-- >>> putStr $ ppFreeP (fromFree' sampleExp)
-- 0 	   [ 1, 0, 0, 	 Op (() :+: ())
-- 1 	     1, 1, 0, 	 Op (() :+: ())
-- 2 	     1, 1, 1, 	 Op (Lit 1)
-- 2 	     1, 1, 2, 	 Var 'x'
-- 1 	     1, 2, 0, 	 Op (() :+: ())
-- 2 	     1, 2, 3, 	 Op (Lit 2)
-- 2 	     1, 2, 4, 	 Op (() :+: ())
-- 3 	     1, 2, 4, 	 Op (Lit 3)
-- 3 	     1, 2, 4] 	 Var 'y'
ppFreeP :: (Show a, Show (f ()), Acc.Elt a, Acc.Elt (f ())) => FreeP f a -> String
ppFreeP FreeP{..} = unlines $ map f (zip3 ds coords ns)
  where
    f (d, c, n) = d <> " \t " <> c <> " \t " <> n
    ds     = show <$> Acc.toList (Acc.run depths)
    coords = drop 1 (lines (show (Acc.run nodeCoords)))
    ns     = show <$> Acc.toList (Acc.run nodes)

-- | Pretty-print depth-annotated nodes as a ASCII tree-like structure.
--
-- >>> putStr $ ppNodesWithDepth (zip [0,1,2,1,2,3,3,2,3] [0..])
-- 0
-- ├─ 1
-- │  └─ 2
-- └─ 3
--    ├─ 4
--    │  ├─ 5
--    │  └─ 6
--    └─ 7
--       └─ 8
ppNodesWithDepth :: Show a => [(Int, a)] -> String
ppNodesWithDepth xs = unlines $
  zipWith (\t x -> drop 3 $ t <> " " <> show (snd x)) (depthsTree (map fst xs)) xs

-- | Pretty-print an ASCII tree-like structure based on depth list.
--
-- >>> mapM_ putStrLn $ depthsTree [0,1,2,1,2,3,3,2,3]
-- └─
--    ├─
--    │  └─
--    └─
--       ├─
--       │  ├─
--       │  └─
--       └─
--          └─
depthsTree :: [Int] -> [String]
depthsTree = map (<> "─") . scanr1 prev . map defaultLine
  where
    defaultLine i = replicate (3 * i) ' ' <> "└"

    prev i []   = i
    prev i next = zipWith g i (next ++ repeat ' ')
      where
        g ' ' '│' = '│'
        g ' ' '└' = '│'
        g '─' '└' = '┬'
        g '└' '│' = '├'
        g '└' '└' = '├'
        g '─' '├' = '┬'
        g c _     = c

