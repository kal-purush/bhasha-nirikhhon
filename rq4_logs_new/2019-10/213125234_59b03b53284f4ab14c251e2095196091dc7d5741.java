// Java Program to check if a queue 
// of first n natural number can 
// be sorted using a stack 

import java.util.*; 

class  queueAndStack 
{ 
	static Queue<Integer> q = 
					new LinkedList<Integer>(); 
	
	// Function to check if given 
	// queue element can be sorted 
	// into another queue using a stack. 
	static boolean checkSorted(int n) 
	{ 
		Stack<Integer> st = 
					new Stack<Integer>(); 
		int expected = 1; 
		int fnt; 
	
		// while given Queue 
		// is not empty. 
		while (q.size() != 0) 
		{ 
			fnt = q.peek(); 
			q.poll(); 
	
			// if front element is 
			// the expected element 
			if (fnt == expected) 
				expected++; 
	
			else
			{ 
				// if stack is empty, 
				// push the element 
				if (st.size() == 0) 
				{ 
					st.push(fnt); 
				} 
	
				// if top element is less than 
				// element which need to be 
				// pushed, then return fasle. 
				else if (st.size() != 0 && 
						st.peek() < fnt) 
				{ 
					return false; 
				} 
	
				// else push into the stack. 
				else
					st.push(fnt); 
			} 
	
			// while expected element are 
			// coming from stack, pop them out. 
			while (st.size() != 0 && 
				st.peek() == expected) 
			{ 
				st.pop(); 
				expected++; 
			} 
		} 
		
		// if the final expected element 
		// value is equal to initial Queue 
		// size and the stack is empty. 
		if (expected - 1 == n && 
				st.size() == 0) 
			return true; 
	
		return false; 
	} 
	
	// Driver Code 
	public static void main(String args[]) 
	{ 
		q.add(5); 
		q.add(1); 
		q.add(2); 
		q.add(3); 
		q.add(4); 
	
		int n = q.size(); 

		if (checkSorted(n)) 
			System.out.print("Yes"); 
		else
			System.out.print("No"); 
	} 
} 

