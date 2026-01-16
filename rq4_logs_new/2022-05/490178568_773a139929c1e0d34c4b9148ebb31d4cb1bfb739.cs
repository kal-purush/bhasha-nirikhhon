using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StackAndQueue
{
    public class StackUsingLinkList
    {

        Node top;
        public StackUsingLinkList()
        {
            this.top = null;

        }
        public void push(int x)
        {
            Node temp = new Node();
            if (temp == null)
            {
                Console.WriteLine("Heap overflow");
                return;
            }
            else
            {
                temp.data = x;
                temp.link = top;
                top = temp;
            }

        }
        public bool isEmpty()
        {
            return top == null;
        }
        public void display()
        {
            if (top == null)
            {
                Console.Write("\nStack Underflow");
                return;
            }
            else
            {
                Node temp = top;
                while (temp != null)
                {

                    Console.Write("{0}->", temp.data);
                    temp = temp.link;
                }
            }
        }
    }
}