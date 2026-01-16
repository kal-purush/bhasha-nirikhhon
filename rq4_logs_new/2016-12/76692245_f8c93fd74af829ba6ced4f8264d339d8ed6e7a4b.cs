using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClauTextSharp.wiz
{
    public class MovableDeck<T>
    {
        private class Element
        {
            public Element left;
            public Element right;
            public T data;

            public Element() {
                left = null;
                right = null;
            }
            public Element(T val)
            {
                data = val;
                left = null;
                right = null;
            }

        }

        private Element Head; //
        private Element Rear; //
        private int N;

        private void init() /// first...
        {
            N = 0;
            Head = new Element(); // Head,Rear 고정된 Delement를 가진다.
            Rear = new Element();  // 그리고 두개가 서로 가르키게 한다.
            Head.left = null;
            Head.right = Rear;
            Rear.left = Head;
            Rear.right = null;
        }
        public MovableDeck()
        {
            init();
        }

        public bool isEmpty(){
            return (Rear.left == Head ); // Rear의 left가 head를 가르키면 Empty.
        }
        public bool empty() { return isEmpty(); }
        public void push_back(T p)
        {
            N++;
            /////////////////////////////////////////////////////
            // New한다.
            Element temp = new Element(p); //
                                                //
                                                //d double linked list
            temp.right = Rear; // 오른 쪽은 Right.!
            temp.left = Rear.left; // 끝부분이 가르키고 있는 값을 left에 넣는다.
            temp.left.right = temp; // 그리고 temp의 왼쪽 element의 right에 temp를 연결한다.
            Rear.left = temp;
            //////////////////////////////////////////////////////
        }//
        public T pop_front()
        {
            N--;
            ////////////////////////////
            if (isEmpty()) // 예외 검사.
            { throw new Exception("for pop, empty deck.."); }//return NULL;

            Element temp = Head.right.right; //
                                                    // Rear 처리
            T returnTemp = Head.right.data;
            Head.right = null;
            temp.left = Head;
            Head.right = temp;

            return returnTemp;
            //
        }

        public void push_front(T p )
        {
            N++;
            Element temp = new Element(p);

            temp.right = Head.right;
            temp.left = Head;
            temp.right.left = temp;
            Head.right = temp;
        }
        public T pop_back()
        {
            if (isEmpty()) { throw new Exception("for pop, empty deck.."); }
            Element temp = Rear.left.left;

            T returnTemp = Rear.left.data;
            Rear.left = null;
            temp.right = Rear;
            Rear.left = temp;

            N--;
            return returnTemp;
        }
	    
        // after pass this function, other -> has nothing 
        public void add_move(MovableDeck<T> other)
        {
            if( other.empty()) { return; }
            // if other.empty is false!
            //
            other.Head.left = Rear.left;
            Rear.left.right = other.Head.right;
            //
            other.Rear.left.right = Rear;
            Rear.left = other.Rear.left;
            // other clear
            other.Head.right = other.Rear;
            other.Head.left = null;
            other.Rear.left = other.Head;
            other.Rear.right = null;
            // N
            this.N = this.N + other.N;
            other.N = 0;
        }

        public void clear()
        {
            N = 0;
            Head.right = Rear;
            Head.left = null;
            Rear.left = Head;
            Rear.right = null;
        }

        public T front()
        {
            if(empty()) { throw new Exception(" empty but call front!"); }
            return Head.right.data;
        }
        public T back()
        {
            if (empty()) { throw new Exception(" empty but call front!"); }
            return Rear.left.data;
        }

        public int size()
        {
            return N;
        }
    }
}