# Name: Suhrob Hasanov
# OSU Email: hasanovs@oregon
# Course: CS261 - Data Structures
# Assignment: Assignment 2
# Due Date: 2/6/23
# Description: Dynamic Array implementation.


from static_array import StaticArray


class DynamicArrayException(Exception):
    """
    Custom exception class to be used by Dynamic Array
    DO NOT CHANGE THIS CLASS IN ANY WAY
    """
    pass


class DynamicArray:
    def __init__(self, start_array=None):
        """
        Initialize new dynamic array
        DO NOT CHANGE THIS METHOD IN ANY WAY
        """
        self._size = 0
        self._capacity = 4
        self._data = StaticArray(self._capacity)

        # populate dynamic array with initial values (if provided)
        # before using this feature, implement append() method
        if start_array is not None:
            for value in start_array:
                self.append(value)

    def __str__(self) -> str:
        """
        Return content of dynamic array in human-readable form
        DO NOT CHANGE THIS METHOD IN ANY WAY
        """
        out = "DYN_ARR Size/Cap: "
        out += str(self._size) + "/" + str(self._capacity) + ' ['
        out += ', '.join([str(self._data[_]) for _ in range(self._size)])
        return out + ']'

    def __iter__(self):
        """
        Create iterator for loop
        DO NOT CHANGE THIS METHOD IN ANY WAY
        """

        self._index = 0
        return self

    def __next__(self):
        """
        Obtain next value and advance iterator
        DO NOT CHANGE THIS METHOD IN ANY WAY
        """
        try:
            value = self[self._index]
        except DynamicArrayException:
            raise StopIteration

        self._index += 1
        return value

    def get_at_index(self, index: int) -> object:
        """
        Return value from given index position
        Invalid index raises DynamicArrayException
        DO NOT CHANGE THIS METHOD IN ANY WAY
        """
        if index < 0 or index >= self._size:
            raise DynamicArrayException
        return self._data[index]

    def set_at_index(self, index: int, value: object) -> None:
        """
        Store value at given index in the array
        Invalid index raises DynamicArrayException
        DO NOT CHANGE THIS METHOD IN ANY WAY
        """
        if index < 0 or index >= self._size:
            raise DynamicArrayException
        self._data[index] = value

    def __getitem__(self, index) -> object:
        """
        Same functionality as get_at_index() method above,
        but called using array[index] syntax
        DO NOT CHANGE THIS METHOD IN ANY WAY
        """
        return self.get_at_index(index)

    def __setitem__(self, index, value) -> None:
        """
        Same functionality as set_at_index() method above,
        but called using array[index] syntax
        DO NOT CHANGE THIS METHOD IN ANY WAY
        """
        self.set_at_index(index, value)

    def is_empty(self) -> bool:
        """
        Return True is array is empty / False otherwise
        DO NOT CHANGE THIS METHOD IN ANY WAY
        """
        return self._size == 0

    def length(self) -> int:
        """
        Return number of elements stored in array
        DO NOT CHANGE THIS METHOD IN ANY WAY
        """
        return self._size

    def get_capacity(self) -> int:
        """
        Return the capacity of the array
        DO NOT CHANGE THIS METHOD IN ANY WAY
        """
        return self._capacity

    def print_da_variables(self) -> None:
        """
        Print information contained in the dynamic array.
        Used for testing purposes.
        DO NOT CHANGE THIS METHOD IN ANY WAY
        """
        print(f"Length: {self._size}, Capacity: {self._capacity}, {self._data}")

    # -----------------------------------------------------------------------

    def resize(self, new_capacity: int) -> None:
        """
        Resizes the dynamic array based on the value passed.
        """
        # print("self.get_capacity()", self.get_capacity())
        # print("self.length()", self.length())

        # Capacity param check
        if new_capacity <= 0:
            return
        elif new_capacity < self.length():
            return
        else:
            new_array = StaticArray(new_capacity)
            for i in range(0, self.length()):
                new_array[i] = self._data[i]
            self._data = new_array
            self._capacity = new_capacity

    def append(self, value: object) -> None:
        """
        Adds passed value to the end of the dynamic array.
        """

        if self.get_capacity() == self.length():
            # Calling resize and passing double of current cap
            self.resize(self.get_capacity() * 2)
            # print("self.get_capacity() AFTER", self.get_capacity())
            self._data[self.length()] = value
            self._size += 1
        else:
            # print("Else 1")
            # print("cap", self.get_capacity())
            # print("array", self._data)
            for i in range(0, self.get_capacity()):
                if self._data[i] is None:
                    self._data[i] = value
                    self._size += 1
                    return

    def insert_at_index(self, index: int, value: object) -> None:
        """
        Inserts the passed value at a given index into the dynamic array.
        """
        if (index < 0 or index > self.length()):
            raise DynamicArrayException
            return

        if self.get_capacity() == self.length():
            self.resize(self.get_capacity() * 2)
        # This block inserts the value if there is existing value at index
        if (self._data[index] is not None):
            temp = self._data[index]
            self._data[index] = value
            self._size += 1
            # This block reassigns (moves) the items once new val is inserted
            for i in range(index + 1, self.get_capacity()):
                if (i == self.get_capacity() - 1):
                    self._data[i] = temp
                elif (self._data[i + 1] is not None):
                    temp2 = self._data[i]
                    self._data[i] = temp
                    temp = temp2
                else:
                    self._data[i + 1] = self._data[i]
                    self._data[i] = temp
                    break
        # If no value at index
        else:
            self._data[index] = value
            self._size += 1

    def remove_at_index(self, index: int) -> None:
        """
        Removes value at a given index.
        """
        # Index check
        if (index < 0 or index > self.length() - 1):
            raise DynamicArrayException
            return

        # Capacity check and action
        if (self.get_capacity() < 10):
            pass
        elif (self.length() < self.get_capacity() / 4):
            if (self.length() * 2 < 10):
                self.resize(10)
            else:
                self.resize(int(self.length() * 2))
        # Overwriting starting at given index (removal process)
        for i in range(index, self.get_capacity() - 1):
            if (i == self.get_capacity() - 1):
                self._data[i] = None
            else:
                self._data[i] = self._data[i + 1]
        # Decrementing size
        self._size -= 1

    def slice(self, start_index: int, size: int) -> "DynamicArray":
        """
        Returns given size of values starting at a given index.
        """
        # Index check
        if (start_index < 0 or start_index > self.length() - 1):
            raise DynamicArrayException
            return
        # Size check
        if (size < 0 or size > self.length() - start_index):
            raise DynamicArrayException
            return
        # Getting the values for the new array, array of return values
        newArray = DynamicArray()
        for i in range(start_index, size + start_index):
            newArray.append(self._data[i])
        return newArray

    def merge(self, second_da: "DynamicArray") -> None:
        """
        Merges two dynamic arrays.
        """
        for i in range(0, second_da.length()):
            self.append(second_da.get_at_index(i))

    def map(self, map_func) -> "DynamicArray":
        """
        Returns the results after applying the passed function.
        """
        newArray = DynamicArray()
        for i in range(0, self.length()):
            # Applying the passed unction to the each item in array
            newArray.append(map_func(self._data[i]))
        return newArray

    def filter(self, filter_func) -> "DynamicArray":
        """
        Returns the items in the dynamic array that meet the requirements of the filter function.
        """
        newArray = DynamicArray()
        for i in range(0, self.length()):
            if filter_func(self._data[i]):
                # if filter_func returns true, the item at index is added to the return array
                newArray.append(self._data[i])
        return newArray

    def reduce(self, reduce_func, initializer=None) -> object:
        """
        Returns the result of passed function applied to the items in the dynamic array.
        """
        # Check for initializer
        if self.is_empty():
            if initializer:
                return initializer
            else:
                return None
        # If array has only one item
        if (self.length() == 1):
            if initializer is None:
                return self._data[0]
            else:
                return reduce_func(initializer, self._data[0])
        # If no initializer passed
        if initializer is None:
            value = self._data[0]
            for i in range(1, self.length()):
                # Applying reduce function to each item of array
                value = reduce_func(value, self._data[i])
        # initializer was passed
        else:
            value = initializer
            for i in range(0, self.length()):
                # Applying reduce function to each item of array
                value = reduce_func(value, self._data[i])
        return value


def find_mode(arr: DynamicArray) -> (DynamicArray, int):
    """
    Returns the mode and highest frequency number of the dynamic array.
    """
    mode = DynamicArray()
    # grabbing first val of the index and set as mode
    mode.append(arr.get_at_index(0))
    frequency = 1
    temp_frequency = 1

    if (arr.length() == 1):
        return (mode, frequency)
    mode_index = 0
    newMode = None
    for i in range(0, arr.length() - 1):
        if (arr.get_at_index(i) == arr.get_at_index(i + 1)):
            if (mode.length() == 1):
                mode_index = 0

            if (mode.get_at_index(mode_index) == arr.get_at_index(i)):  # same as curr mode
                if (newMode):
                    temp_frequency += 1
                else:
                    frequency += 1

            elif (mode.get_at_index(mode_index) == arr.get_at_index(0) and frequency == 1):  # diff from default mode
                mode.set_at_index(mode_index, arr.get_at_index(i))
                frequency += 1
            elif (mode.get_at_index(mode_index) != arr.get_at_index(i)):  # diff from current mode
                temp_frequency += 1

            if (frequency < temp_frequency):

                frequency = temp_frequency
                # clearing the mode array off existing less frequent modes

                temp = mode.get_at_index(0)
                mode.set_at_index(0, arr.get_at_index(i))
                # Deleting previous modes since new higher mode was identified
                while (mode.length() >= 2):
                    mode.remove_at_index(1)
                mode_index = 0
                temp_frequency = 1
                newMode = False
                # Adjusting the capacity as the mode array changes
                if (mode.length() < mode.get_capacity() / 4):
                    mode.resize((round(mode.get_capacity() / 4)) * 2)
            elif (frequency == temp_frequency):
                mode_index += 1
                mode.insert_at_index(mode_index, arr.get_at_index(i))
                newMode = True
        else:
            temp_frequency = 1
            if (frequency == temp_frequency):
                mode.append(arr.get_at_index(i + 1))
                mode_index += 1
                newMode = True

    return (mode, frequency)


# ------------------- BASIC TESTING -----------------------------------------


if __name__ == "__main__":

    # print("\n# resize - example 1")
    # da = DynamicArray()
    #
    # # print dynamic array's size, capacity and the contents
    # # of the underlying static array (data)
    # da.print_da_variables()
    # da.resize(8)
    # da.print_da_variables()
    # da.resize(2)
    # da.print_da_variables()
    # da.resize(0)
    # da.print_da_variables()
    #
    # print("\n# resize - example 2")
    # da = DynamicArray([1, 2, 3, 4, 5, 6, 7, 8])
    # print(da)
    # da.resize(20)
    # print(da)
    # da.resize(4)
    # print(da)
    #
    # print("\n# append - example 1")
    # da = DynamicArray()
    # da.print_da_variables()
    # da.append(1)
    # da.print_da_variables()
    # print(da)
    #
    # print("\n# append - example 2")
    # da = DynamicArray()
    # for i in range(9):
    #     da.append(i + 101)
    #     print(da)

    # print("\n# append - example 3")
    # da = DynamicArray()
    # for i in range(600):
    #     da.append(i)
    # print(da.length())
    # print(da.get_capacity())
    #
    # print("\n# insert_at_index - example 1")
    # da = DynamicArray([100])
    # print(da)
    # da.insert_at_index(0, 200)
    # da.insert_at_index(0, 300)
    # da.insert_at_index(0, 400)
    # print(da)
    # da.insert_at_index(3, 500)
    # print(da)
    # da.insert_at_index(1, 600)
    # print(da)
    #
    # print("\n# insert_at_index example 2")
    # da = DynamicArray()
    # try:
    #     da.insert_at_index(-1, 100)
    # except Exception as e:
    #     print("Exception raised:", type(e))
    # da.insert_at_index(0, 200)
    # try:
    #     da.insert_at_index(2, 300)
    # except Exception as e:
    #     print("Exception raised:", type(e))
    # print(da)
    #
    # print("\n# insert at index example 3")
    # da = DynamicArray()
    # for i in range(1, 10):
    #     index, value = i - 4, i * 10
    #     try:
    #         da.insert_at_index(index, value)
    #     except Exception as e:
    #         print("Cannot insert value", value, "at index", index)
    # print(da)

    # print("\n# remove_at_index - example 1")
    #
    #
    # da = DynamicArray(["f", "h", "yFNd^qmamM", "oyCJ_oZF", "Cytf^", "MJx", "Ia`^", "[^XWIIG"])
    # print(da)
    # da.remove_at_index(2)
    # print(da)
    # print()
    # da = DynamicArray(["a1", "b2", "c3", "d4", "e5", "f6", "g7", "h8"])
    # print(da)
    # da.remove_at_index(2)
    # print(da)
    # print()
    # da = DynamicArray([10, 20, 30, 40, 50, 60, 70, 80])
    # print(da)
    # da.remove_at_index(4)
    # print(da)
    # print()
    # print()
    # print()
    #
    #
    #
    #
    #
    #
    #
    # da = DynamicArray([10, 20, 30, 40, 50, 60, 70, 80])
    # print(da)
    # da.remove_at_index(0)
    # print(da)
    # da.remove_at_index(6)
    # print(da)
    # da.remove_at_index(2)
    # print(da)
    #
    #
    #
    #
    #
    # print("\n# remove_at_index - example 2")
    # da = DynamicArray([1024])
    # print(da)
    # for i in range(17):
    #     da.insert_at_index(i, i)
    # print(da.length(), da.get_capacity())
    # for i in range(16, -1, -1):
    #     da.remove_at_index(0)
    # print(da)
    #
    # print("\n# remove_at_index - example 3")
    # da = DynamicArray()
    # print(da.length(), da.get_capacity())
    # [da.append(1) for i in range(100)]  # step 1 - add 100 elements
    # print(da.length(), da.get_capacity())
    # [da.remove_at_index(0) for i in range(68)]  # step 2 - remove 68 elements
    # print(da.length(), da.get_capacity())
    # da.remove_at_index(0)  # step 3 - remove 1 element
    # print(da.length(), da.get_capacity())
    # da.remove_at_index(0)  # step 4 - remove 1 element
    # print(da.length(), da.get_capacity())
    # [da.remove_at_index(0) for i in range(14)]  # step 5 - remove 14 elements
    # print(da.length(), da.get_capacity())
    # da.remove_at_index(0)  # step 6 - remove 1 element
    # print(da.length(), da.get_capacity())
    # da.remove_at_index(0)  # step 7 - remove 1 element
    # print(da.length(), da.get_capacity())
    #
    # for i in range(14):
    #     print("Before remove_at_index(): ", da.length(), da.get_capacity(), end="")
    #     da.remove_at_index(0)
    #     print(" After remove_at_index(): ", da.length(), da.get_capacity())
    #
    # print("\n# remove at index - example 4")
    # da = DynamicArray([1, 2, 3, 4, 5])
    # print(da)
    # for _ in range(5):
    #     da.remove_at_index(0)
    #     print(da)

    # print("\n# slice example 1")
    # da = DynamicArray([1, 2, 3, 4, 5, 6, 7, 8, 9])
    # da_slice = da.slice(1, 3)
    # print(da, da_slice, sep="\n")
    # da_slice.remove_at_index(0)
    # print(da, da_slice, sep="\n")
    #
    # print("\n# slice example 2")
    # da = DynamicArray([10, 11, 12, 13, 14, 15, 16])
    # print("SOURCE:", da)
    # slices = [(0, 7), (-1, 7), (0, 8), (2, 3), (5, 0), (5, 3), (6, 1), (6, -1)]
    # for i, cnt in slices:
    #     print("Slice", i, "/", cnt, end="")
    #     try:
    #         print(" --- OK: ", da.slice(i, cnt))
    #     except:
    #         print(" --- exception occurred.")

    # print("\n# merge example 1")
    # da = DynamicArray([1, 2, 3, 4, 5])
    # da2 = DynamicArray([10, 11, 12, 13])
    # print(da)
    # da.merge(da2)
    # print(da)
    #
    # print("\n# merge example 2")
    # da = DynamicArray([1, 2, 3])
    # da2 = DynamicArray()
    # da3 = DynamicArray()
    # da.merge(da2)
    # print(da)
    # da2.merge(da3)
    # print(da2)
    # da3.merge(da)
    # print(da3)

    # print("\n# map example 1")
    # da = DynamicArray([1, 5, 10, 15, 20, 25])
    # print(da)
    # print(da.map(lambda x: x ** 2))
    #
    # print("\n# map example 2")
    #
    #
    # def double(value):
    #     return value * 2
    #
    #
    # def square(value):
    #     return value ** 2
    #
    #
    # def cube(value):
    #     return value ** 3
    #
    #
    # def plus_one(value):
    #     return value + 1
    #
    #
    # da = DynamicArray([plus_one, double, square, cube])
    # for value in [1, 10, 20]:
    #     print(da.map(lambda x: x(value)))

    # print("\n# filter example 1")
    #
    #
    # def filter_a(e):
    #     return e > 10
    #
    #
    # da = DynamicArray([1, 5, 10, 15, 20, 25])
    # print(da)
    # result = da.filter(filter_a)
    # print(result)
    # print(da.filter(lambda x: (10 <= x <= 20)))
    #
    # print("\n# filter example 2")
    #
    #
    # def is_long_word(word, length):
    #     return len(word) > length
    #
    #
    # da = DynamicArray("This is a sentence with some long words".split())
    # print(da)
    # for length in [3, 4, 7]:
    #     print(da.filter(lambda word: is_long_word(word, length)))
    # print()
    # values = ["bllokce", "oiwrazylru", "vxfsnlzn", "ra", "tqhbsrhpjm", "t", "bikrqesrn", "weux", "jdcawpshvm", "oloesc"]
    # da = DynamicArray(values)
    # print(da)
    # print("bllokceoiwrazylruvxfsnlznratqhbsrhpjmtbikrqesrnweuxjdcawpshvmoloesc")
    # print(da.reduce(lambda x, y:(x + y)))
    #
    #
    # print("\n# reduce example 1")
    # values = [100, 5, 10, 15, 20, 25]
    # da = DynamicArray(values)
    # print(da)
    # print(da.reduce(lambda x, y: (x // 5 + y ** 2)))
    # print(da.reduce(lambda x, y: (x + y ** 2), -1))
    #
    # print("\n# reduce example 2")
    # da = DynamicArray([100])
    # print(da.reduce(lambda x, y: x + y ** 2))
    # print(da.reduce(lambda x, y: x + y ** 2, -1))
    # da.remove_at_index(0)
    # print(da.reduce(lambda x, y: x + y ** 2))
    # print(da.reduce(lambda x, y: x + y ** 2, -1))

    print("\n# find_mode - example 1")
    test_cases = (
        ["HvocSl", " dCzZHEiHsc", "fLYqFsczRm", "ppBhWP", "ppBhWP", "svk", "uz", "uz", "uz"],
        ["zgvNS", "s", "ms", " kmroiPTYd", "fhnAFNy", "awfUaHygN", "U", "TDYSb", "GDaiXMEood", "EoD", "BsDL", "BsDL"],
        [21, 195, 346, 530, 752, 752],
        ["Apple", "Banana", "Banana", "Carrot", "Carrot",
         "Date", "Date", "Date", "Eggplant", "Eggplant", "Eggplant",
         "Fig", "Fig", "Grape"],
        [1, 1, 2, 3, 3, 4],
        [1, 2, 3, 4, 5]

    )

    for case in test_cases:
        da = DynamicArray(case)
        mode, frequency = find_mode(da)
        print(f"{da}\nMode: {mode}, Frequency: {frequency}\n")

    case = [4, 3, 3, 2, 2, 2, 1, 1, 1, 1]
    da = DynamicArray()
    for x in range(len(case)):
        da.append(case[x])
        mode, frequency = find_mode(da)
        print(f"{da}\nMode: {mode}, Frequency: {frequency}")