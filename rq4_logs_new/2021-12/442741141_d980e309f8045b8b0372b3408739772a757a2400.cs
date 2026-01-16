namespace LinkedList
{

    public class DynamicList<T>
    {
        private class ListNode //holds element and pointer to next element
        {
            public T Element { get; set; }
            public ListNode NextNode { get; set; }

            public ListNode(T element)
            {
                Element = element;
                NextNode = null;
            }

            public ListNode(T element, ListNode prevNode)
            {
                Element = element;
                prevNode.NextNode = this;
            }
        }

        private ListNode _head; //pointer to first element
        private ListNode _tail; //pointer last element
        private int _count; //element counter

        public DynamicList()
        {
            _head = null;
            _tail = null;
            _count = 0;
        }

        public int Count
        {
            get
            {
                return _count;
            }
        }

        public T this[int index]
        {
            get
            {

                ThrowIfIndexOutOfRange(index);

                ListNode currentNode = _head;
                for (int i = 0; i < index; i++)
                {
                    currentNode = currentNode.NextNode;
                }

                return currentNode.Element;

            }
            set
            {
                ThrowIfIndexOutOfRange(index);

                ListNode currentNode = this._head;
                for (int i = 0; i < index; i++)
                {
                    currentNode = currentNode.NextNode;
                }
                currentNode.Element = value;
            }

        }

        public void Add(T item)
        {
            if (_head == null)
            {
                _head = new ListNode(item); //create new head and tail
                _tail = _head;
            }
            else
            {
                ListNode newNode = new ListNode(item, _tail); //non empty list append after tail
                _tail = newNode;
            }
            _count++;
        }

        public T RemoveAt(int index)
        {
            ThrowIfIndexOutOfRange(index);

            //Find the element at the specified index
            int currentIndex = 0;
            ListNode currentNode = _head;
            ListNode prevNode = null;
            while (currentIndex < index) //gavirben indexamde da im elements davicehrt
            {
                prevNode = currentNode;
                currentNode = currentNode.NextNode;
                currentIndex++;
            }

            //remove the found element from the list of nodes
            RemoveListNode(currentNode, prevNode);

            return currentNode.Element;
        }

        private void RemoveListNode(ListNode node, ListNode prevNode)
        {
            _count--;
            if (_count == 0)
            {
                _head = null;
                _tail = null;
            }
            else if (prevNode == null)
            {
                //head node was removed so update the head
                _head = node.NextNode;
            }
            else
            {
                // in the middle or at the end of the list
                prevNode.NextNode = node.NextNode;
            }
        }



        public void PrintList()
        {
            while (_head != null)
            {
                Console.WriteLine(_head.Element);
                _head = _head.NextNode;
            }
        }

        public void ThrowIfIndexOutOfRange(int index)
        {
            if (index >= _count || index < 0)
            {
                throw new ArgumentOutOfRangeException($"Invalid index: {index}");
            }
        }
    }


}