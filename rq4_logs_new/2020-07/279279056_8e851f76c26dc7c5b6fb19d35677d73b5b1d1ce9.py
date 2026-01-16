class PriorityQueue:

    def __init__(self, arr=(), key=lambda x: x, cmp=lambda x, y: x < y):
        """
            Implementing Priority Queue using Heap Data Structure.
        :param arr: * Initial elements of to be inserted in the heap.
                    * Type : List
                    * Default : Empty List.

        :param key: * Function to evaluate the element of the heap. Default : element itself.
                    * Type : Method or lambda function:
                            -> Params : One parameter:
                                    @ Param1 : Type -> Object : Heap element
                            -> Return type : Numeric.
                    * Default : Identity function

        :param cmp: * Function to define the precedence between 2 elements.
                    * Type :  Method or lambda function:
                            -> Params : Two parameters:
                                    @ Param1: Type -> Object : Heap element
                                    @ Param2: Type -> Object : Heap element
                            -> Return type : boolean.
                                    Return True -> if first param precedes second.
                                          False -> if second param precedes first.
                    * Default : (x, y): return x < y ; [Min Heap]
        """

        self._arr = list(arr)
        self._key = key
        self._cmp = cmp
        self.__heapify()

    def __compare(self, x, y):
        return self._cmp(self._key(self._arr[x]), self._key(self._arr[y]))

    @staticmethod
    def __has_parent(index):
        return (index-1)//2 >= 0

    @staticmethod
    def __get_parent(index):
        return (index-1)//2

    def __has_left_child(self, index):
        return 2*index + 1 < len(self._arr)

    def __has_right_child(self, index):
        return 2 * index + 2 < len(self._arr)

    @staticmethod
    def __get_left_child(index):
        return 2*index + 1

    @staticmethod
    def __get_right_child(index):
        return 2*index + 2

    def __swap(self, i, j):
        temp = self._arr[i]
        self._arr[i] = self._arr[j]
        self._arr[j] = temp

    def __heapify_up(self, index):

        while self.__has_parent(index):
            if self.__compare(index, self.__get_parent(index)):
                self.__swap(self.__get_parent(index), index)
                index = self.__get_parent(index)
            else:
                break

    def __heapify_down(self, index):

        while self.__has_left_child(index):

            if self.__has_right_child(index) and \
                    self.__compare(self.__get_right_child(index), self.__get_left_child(index)) \
                    and self.__compare(self.__get_right_child(index), index):

                self.__swap(self.__get_right_child(index), index)
                index = self.__get_right_child(index)

            elif self.__compare(self.__get_left_child(index), index):

                self.__swap(self.__get_left_child(index), index)
                index = self.__get_left_child(index)

            else:
                break

    def __heapify(self):

        if len(self._arr) <= 1:
            return
        # first non leaf node = ((n-1)-1)//2 -> n//2 - 1
        for i in range(len(self._arr) // 2 - 1, -1, -1):
            self.__heapify_down(i)

    def is_empty(self):
        """
        Check whether the Heap is empty or not.
        :return: Type - Boolean
        """

        return len(self._arr) == 0

    def peek(self):
        """
         Returns the top node of the heap.
         Returns None if the heap is empty.
        :return: Type - Object : Heap Element
        """
        if len(self._arr) > 0:
            return self._arr[0]

    def push(self, val):
        """
         Insert element into the heap.
         Time Complexity : O(log(n))
        :param val: Type - Object : Heap Element
        :return: None
        """
        self._arr.append(val)
        self.__heapify_up(len(self._arr) - 1)

    def pop(self):
        """
         Pops most prioritised element from the Heap.
         Time Complexity : O(log(n))
        :return: None
        """
        if len(self._arr) == 0:
            return
        self.__swap(0, len(self._arr) - 1)
        item = self._arr.pop()
        self.__heapify_down(0)
        return item