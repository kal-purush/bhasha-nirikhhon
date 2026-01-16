using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibrary
{
    public class BreadthFirstIterator : LightHTMLIterator
    {
        private Queue<LightNode> queue;
        private LightNode current;

        public BreadthFirstIterator(LightNode root)
        {
            queue = new Queue<LightNode>();
            queue.Enqueue(root);
        }

        public override LightNode Current => current;

        public override bool MoveNext()
        {
            if (queue.Count == 0)
                return false;

            current = queue.Dequeue();

            if (current is LightElementNode elementNode)
            {
                foreach (LightNode child in elementNode.Children)
                {
                    queue.Enqueue(child);
                }
            }

            return true;
        }

        public override void Reset()
        {
            queue.Clear();
        }
    }
}