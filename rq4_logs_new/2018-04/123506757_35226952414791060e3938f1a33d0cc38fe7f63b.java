
/**
 * @author Amy Larson amlarson10@dmacc.edu 2018-03-15 updated 4/2/18CIS152 This
 *         class creates a queue array that uses queue methods
 */
public class ArrayQueue extends Queue {

	private int front;
	private int rear;
	private int nItems;
	private JobListing[] qArray;
	
	/**
	 *Constructor for ArrayQueue
	 * @param queueName The name that will appear in instructions and on buttons. No
	 *            spaces before or after. May include "The " at the start.
	 * @param maxSize  must be greater than 0
	 */
	public ArrayQueue(String queueName, int maxSize) {
		super(queueName, maxSize);
		qArray = new JobListing[getMaxSize()];
		front = 0;
		rear = -1;
		nItems = 0;
	}

	/**
	 * adds a new item to the queue, queue is modified.
	 * @see Queue#enqueue(JobListing)
	 * @param item
	 * @return
	 */
	public boolean enqueue(JobListing item) {

		if (isFull()) {
			return false;
		} else {
			if (rear == getMaxSize() - 1) { // check for wraparound
				rear = -1;
			}
			qArray[++rear] = item; // increment rear and insert
			nItems++; // add count of items
			return true;
		}
	}


	/**removes and returns an item, queue is modified.
	 * @see Queue#dequeue()
	 * @return
	 */
	public JobListing dequeue() {
		JobListing item = qArray[front++]; // get item and increment front
		if (front == getMaxSize()) { // check for wraparound
			front = 0;
		}
		nItems--; // decrease count of items
		return item;
	}

	/**
	 * prints the front element, queue is not modified.
	 * @see Queue#peek()
	 * @return item at front of queue
	 */
	public JobListing peek() {
		return qArray[front];
	}

	/**
	 * returns a boolean and tests for an empty queue, queue is not modified.
	 * @see Queue#isEmpty()
	 * @return
	 */
	public boolean isEmpty() { // true if queue is empty
		return (nItems == 0);
	}


	
	/**
	 * checks if queue is full
	 * @see Queue#isFull()
	 * @return true if full
	 */
	public boolean isFull() { // true if queue is full
		return (nItems == getMaxSize());
	}

	/**
	 * returns the int size of the queue, queue is not modified
	 * @see Queue#size()
	 * @return
	 */
	public int size() {
		return nItems;
	}


	/**
	 * prints the queue from front to rear, queue is not modified.
	 * @see Queue#print()
	 */
	public void print() {
		int i = front;
		System.out.println("Display queue front to back:");
		while (i != rear + 1) {
			System.out.println("-" + qArray[i].toString());
			if (i == getMaxSize()) {
				i = -1;
			}
			i++;
		}
	}

}