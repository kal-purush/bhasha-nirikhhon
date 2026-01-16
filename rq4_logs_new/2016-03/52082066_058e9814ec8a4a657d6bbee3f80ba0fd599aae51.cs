using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ListNamespace
{
    /// <summary>
    /// class for list
    /// </summary>
    public class List
    {
        /// <summary>
        /// class for element of stack
        /// </summary>
        class Element
        {
            public int Value { get; set; }
            public Element Next { get; set; }

            /// <summary>
            /// constructor for stack element
            /// </summary>
            /// <param name="_value"></param>
            /// <param name="next"></param>
            public Element(int value = 0, Element next = null)
            {
                this.Value = value;
                this.Next = next;
            }
        }
        private Element head = null;// new Element();
        private int _count;

        /// <summary>
        /// stack constructor 
        /// </summary>
        public List()
        {
            _count = -1;
        }

        /// <summary>
        /// add element in stack
        /// </summary>
        /// <param name="value"></param> 
        public void Push(int value)
        {
            head = new Element(value, head);
            ++_count;
        }

        /// <summary>
        /// get element from position
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public int GetElement(int index)
        {
            Element temp = head;
            for (int i = 0; temp.Next != null & i < index - 1; ++i)
            {
                temp = temp.Next;
            }
            return temp.Value;
        }

        /// <summary>
        /// add element in postion
        /// </summary>
        /// <param name="index"></param>
        /// <param name="value"></param>
        public void InIndex(int index, int value)
        {
            ++_count;
            if ((head == null) || (index == 0))
            {
                head = new Element(value, head);
                return;
            }
            Element newElem = new Element(value, null);
            Element temp = head;
            for(int i = 0; temp.Next != null & i < index - 1; ++i)
            {
                temp = temp.Next;
            }
            newElem.Next = temp.Next;
            temp.Next = newElem; 
        }

        /// <summary>
        /// delete element from list
        /// </summary>
        /// <returns></returns>
        public int Pop(int elem)
        {
            if (head == null)
                return 0;
            if (head.Value == elem)
            {
                int tmp = head.Value;
                head = head.Next;
                --_count;
                return tmp;
            }
            Element temp = head;
            Element secondTemp = head;
            while (temp.Next != null)
            {
                temp = temp.Next;
                if (temp.Value == elem)
                {
                    secondTemp.Next = temp.Next;
                    --_count;
                    return temp.Value;
                }
                secondTemp = temp;
            }
            return 0;
        }

        /// <summary>
        /// remove element from position
        /// </summary>
        /// <param name="index"></param>
        public void RemomeAt(int index)
        {
            if (head == null)
                return;
            if (index == 0)
            {
                int tmp = head.Value;
                head = head.Next;
                --_count;
            }
            Element temp = head;
            Element secondTemp = head;
            int i = 1;
            while (temp.Next != null)
            {
                temp = temp.Next;
                if (i == index)
                {
                    secondTemp.Next = temp.Next;
                    --_count;
                }
                secondTemp = temp;
                ++i;
            }
            return;
        }

        /// <summary>
        /// print all stack
        /// </summary>
        public void Print()
        {
            Element tmp = head;
            do
            {
                Console.Write(tmp.Value + " ");
                tmp = tmp.Next;
            } while (tmp != null);
        }

        /// <summary>
        /// find element in list
        /// </summary>
        /// <param name="element"></param>
        /// <returns></returns>
        public bool Find(int element)
        {
            Element tmp = head;
            for (int i = 0; i <= _count; ++i)
            {
                if (tmp.Value == element)
                {
                    return true;
                }
                tmp = tmp.Next;
            }
            return false;
        }
    }
}