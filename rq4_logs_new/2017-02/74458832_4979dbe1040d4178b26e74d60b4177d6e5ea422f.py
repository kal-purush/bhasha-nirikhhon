import unittest

class FastBoolStack:
    """ A faster bool stack. """

    def __init__(self):
        """ Creates a FastBoolStack
                    
        """        
        self._elements = []
        
    def size(self):
        return len(self._elements)
        
    def is_empty(self):
        return len(self._elements) == 0
        
    def pop(self):
        """ Removes the element at the top of the stack and returns it.
            
            If the stack is empty, raises an IndexError
        """
        
        if (len(self._elements) == 0):
            raise IndexError("Stack is empty!")            

        item = self._elements.pop()                
        return item

    def push(self, item):        
        """ Inserts a boolean item in the stack
            
            If item is not a boolean, raises a ValueError
        """        
        if (type(item) is not bool):
            raise ValueError("Invalid object! Expected a bool, found instead " + str(item))

        self._elements.append(item)
        
    def peek(self):
        """
            Returns the first element in the stack, without modifying the stack.        
        
            If stack is empty, raises an IndexError.
        """        
        
        if (len(self._elements) == 0):
            raise IndexError("Stack is empty!")            
        else:
            return self._elements[-1]
        
    
    def __str__(self):
        return "FastBoolStack:  " + " elements=" + str(self._elements) 
                

    def true_count(self):
        """ Return the number of elements which are True 

            ***   MUST EXECUTE IN O(1)  ***
        """
        
        raise Exception("TODO IMPLEMENT ME !")
        
        

class FastBoolStackTest(unittest.TestCase):
               
    def test_size(self):
        s = FastBoolStack()
        self.assertEqual(s.size(), 0)
        s.push(True)
        self.assertEqual(s.size(), 1)
        s.pop()
        self.assertEqual(s.size(), 0)

    
    def test_is_empty(self):
        s = FastBoolStack()
        self.assertTrue(s.is_empty())
        s.push(True)
        self.assertFalse(s.is_empty())

    def test_peek_empty(self):
        s = FastBoolStack()
        with self.assertRaises(IndexError):
            s.peek()

    def test_peek_one(self):
        s = FastBoolStack()        
        s.push(True)
        self.assertEqual(s.peek(), True)
        self.assertEqual(s.peek(), True)  # testing peek is not changing the stack
        self.assertEqual(s.size(), 1)     
        
    def test_pop_empty(self):
        s = FastBoolStack() 
        with self.assertRaises(IndexError):
            s.pop()
            
    def test_pop_one(self):
        s = FastBoolStack() 
        with self.assertRaises(IndexError):
            s.pop()
        s.push(True)        
        self.assertEqual(s.pop(), True)
        self.assertEqual(s.size(), 0)
        
    def test_pop_two(self):
        s = FastBoolStack()     
        with self.assertRaises(IndexError):
            s.pop()
        s.push(True)
        s.push(False)        
        self.assertEqual(s.pop(), False)
        self.assertEqual(s.size(), 1)
        self.assertEqual(s.pop(), True)
        self.assertEqual(s.size(), 0)
        with self.assertRaises(IndexError):
            s.pop()
                        
    def test_push_non_bool(self):
        s = FastBoolStack()        
        with self.assertRaises(ValueError):
            s.push("evil string")
    
    def test_push(self):
        s = FastBoolStack()        
        self.assertEqual(s.size(), 0)
        s.push(True)
        self.assertEqual(s.size(), 1)
        self.assertEqual(s.peek(), True)
        s.push(False)
        self.assertEqual(s.size(), 2)
        self.assertEqual(s.peek(), False)
        s.push(True) 
        self.assertEqual(s.size(), 3)
        self.assertEqual(s.peek(), True)

    def test_str(self):
        s = FastBoolStack()
        s.push(True)
        s.push(False)        
        self.assertTrue( 'True' in str(s))
        self.assertTrue( 'False' in str(s))
        self.assertTrue( 'FastBoolStack' in str(s))

            
class TestTrueCount(unittest.TestCase):

    def test_true_count_fast_zero(self):
        s = FastBoolStack()
        self.assertEqual(s.true_count(), 0)

    def test_true_count_fast_push(self):
        s = FastBoolStack()
        self.assertEqual(s.true_count(), 0)
        s.push(True)
        self.assertEqual(s.true_count(), 1)
        s.push(False)
        self.assertEqual(s.true_count(), 1)
        s.push(True)
        self.assertEqual(s.true_count(), 2)

    
    def test_true_count_fast_pushpop(self):
        s = FastBoolStack()
        self.assertEqual(s.true_count(), 0)
        s.push(True)
        self.assertEqual(s.true_count(), 1)
        s.push(False)
        self.assertEqual(s.true_count(), 1)
        s.push(True)
        self.assertEqual(s.true_count(), 2)
        s.pop()
        self.assertEqual(s.true_count(), 1)        
        s.pop()
        self.assertEqual(s.true_count(), 1)                
        s.pop()
        self.assertEqual(s.true_count(), 0)        
        with self.assertRaises(IndexError):
            s.pop()   