using System.Collections.Generic;

namespace AIHomeworks_DrAbrishami
{
    public class DFS_Fringe : Fringe
    {
        // it's basically a stack
        private Stack<knightsState> stack;

        public DFS_Fringe()
        {
            stack = new Stack<knightsState>();
        }
        
        public override knightsState Pop()
        {
            return stack.Pop();
        }

        public override void Append(knightsState ks)
        {
            stack.Push(ks);
        }
    }
}