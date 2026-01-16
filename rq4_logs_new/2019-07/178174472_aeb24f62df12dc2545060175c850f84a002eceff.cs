using System.Collections.Generic;
using MononokeEngine.Utils.DataStructures.Heaps;

namespace MononokeEngine.Utils.Pathfinding.AStar
{
    internal class AStarBinaryHeap : IBinaryHeap<AStarNode>
    {
        public int Count => _heap.Count;
        public bool IsEmpty => Count == 0;
		
		
		
        private readonly List<AStarNode> _heap;


        public AStarBinaryHeap()
        {
            _heap = new List<AStarNode>();
        }
        
        
        public void Add(AStarNode element)
        {
            if (!Contains(element))
            {			
                _heap.Add(element);
                FilterUp(_heap.Count - 1);
            }
        }

        public AStarNode Poll()
        {
            if (IsEmpty)
                return null;
		
            // Get First
            AStarNode element = RemoveNodeAt(0);
		
            // Reorder
            if (_heap.Count > 0)
            {
                _heap.Insert(0, RemoveNodeAt(Count - 1));
                FilterDown(0);
            }
		
            return element;
        }

        public AStarNode Peek()
        {
            if (IsEmpty)
                return null;
			
            return _heap[0];
        }

        
        /// <summary>
        /// O(1) implementation
        /// </summary>
        /// <param name="element"></param>
        /// <returns></returns>
        public bool Contains(AStarNode element)
        {
            int i = element.HeapIndex;
            if (!IsValidIndex(i))
                return false;
            
            return _heap[i] == element;
        }

        public bool Remove(AStarNode element)
        {
            if (!Contains(element))
                return false;

            int index = element.HeapIndex;
            _heap[index].MinusInfiniteWeight = true;
            FilterUp(index);

            Poll();
			
            return true;
        }
        
        
        
        
        
        
        
        
        
        
        

        private void FilterUp(int pos)
        {
            if (!IsValidIndex(pos))
                return;
		
            var node = _heap[pos];
            int comp = node.CompareTo( _heap[GetParent(pos)] );
		
            while (pos > 0 && comp < 0)
            {
                Swap(pos, GetParent(pos));
                pos = GetParent(pos);
                comp = node.CompareTo( _heap[GetParent(pos)] );
            }			
        }
		
		
        private void FilterDown(int pos)
        {
            if (!IsValidIndex(pos))
                return;

            while (!IsLeaf(pos))
            {
                var node = _heap[pos];
                int posL = GetLeftChild(pos);
                int posR = GetRightChild(pos);
			
                int childComp; // The comparison between the left child and the right child
                if (!IsValidIndex(posR)) childComp = -1; // -1 = left
                else childComp = _heap[posL].CompareTo( _heap[posR] );
			
                if (childComp < 0) // Swapping with left child
                {
                    if (_heap[posL].CompareTo(node) < 0)
                    {
                        Swap(pos, posL);
                        pos = GetLeftChild(pos);
                    }
                    else break;
                }
                else // Swapping with the right child
                {
                    if (_heap[posR].CompareTo(node) < 0)
                    {
                        Swap(pos, posR);
                        pos = GetRightChild(pos);
                    }
                    else break;
                }		
			
            }
        }
        
        
        
        
        
        
        
        
        
        private void Swap(int indexA, int indexB)
        {
            var nodeA = _heap[indexA];
            var nodeB = _heap[indexB];
            
            _heap[indexA] = nodeB;
            nodeB.HeapIndex = indexA;
            
            _heap[indexB] = nodeA;
            nodeA.HeapIndex = indexB;
        }

        
        private AStarNode RemoveNodeAt(int index)
        {
            var element = _heap[index];
            _heap.RemoveAt(index);
            return element;
        }
		
        private int GetParent(int pos)
        {
            return (pos - 1)/2;
        }
	
        private int GetLeftChild(int pos)
        {
            return 2*pos + 1;
        }
	
        private int GetRightChild(int pos)
        {
            return 2*pos + 2;
        }
	
        private bool IsLeaf(int pos)
        {
            bool left = GetLeftChild(pos) >= _heap.Count;
            bool right = GetRightChild(pos) >= _heap.Count;
		
            return right && left;
        }
	
        private bool IsValidIndex(int pos)
        {
            return pos >= 0 && pos < _heap.Count;
        }
    }
}