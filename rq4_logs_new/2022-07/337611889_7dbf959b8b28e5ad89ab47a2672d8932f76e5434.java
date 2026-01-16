package HW3;

public class HW3<E> {
	
	Object[] rawData;
	// You may add whatever fields/methods that are deemed necessary
	public int size;
	
	public HW3() {
		size = 0;
	}
	
	/*
	 * TODO: implement the following methods with appropriate header comments. 
	 */
	
	/*
	 * insertFront(E obj)
	 * First check the array is empty. When array is empty just increase one for the size => constant time. 
	 * When the array has elements already, make new rawData[size + 1] => constant time.
	 * And move all the elements from old array into new rawData start with index = 1 one by one => linear time. 
	 * Finally put obj to rawData[0] => constant time.
	 * So time complexity is linear complexity which affected by the size of rawData.
	 * O(N)
	 */
	public void insertFront(E obj) {
		if(size == 0) {
			rawData = new Object[size + 1];
		}else if (size > 0) {
			Object[] temp = rawData;
			rawData = new Object[size + 1];
			
			for (int i = 0; i < size; i++) {
				rawData[i + 1] = temp[i];
			}
			temp = null;
		}
		
		rawData[0] = obj;
		size++;
	}
	
	/*
	 * insertRear(E obj)
	 * First check the array is empty. When array is empty just increase one for the size => constant time. 
	 * When the array has elements already, make new rawData[size + 1] => constant time. 
	 * And move all the elements from old array into new rawData start with index = 0 one by one => linear time.
	 * And then put obj in the last of rawData => constant time
	 * So time complexity is constant time.
	 * O(N)
	 */
	public void insertRear(E obj) {
		if (size == 0) {
			rawData = new Object[size + 1];
		}else if (size > 0) {
			Object[] temp = rawData;
			rawData = new Object[size + 1];
			for (int i = 0; i < size; i++) {
				rawData[i] = temp[i];
			}
			temp = null;
		}
		
		rawData[size] = obj;
		size++;
	}
	
	
	/*
	 * Return the deleted element.
	 * Your code should also take appropriate action when deleting the front item is not possible.
	 * (same for deleteRear(), getFront(), and getRear()) 
	 */
	
	/*
	 * deleteFront(E obj)
	 * First check the array is empty. When array is empty, throw exception => constant time. 
	 * When the array has elements already, make new rawData[size - 1] => constant time.
	 * Move all the elements from the old array to sizeddown rawData one by one (except front value) => linear time.
	 * So time complexity is linear complexity which affected by the size of rawData.
	 * O(N)
	 */
	public E deleteFront() {
		if (size <= 0) {
			return (E) "deleting the front item is not possible.";
		}
		E del_value = getFront();
		
		Object[] temp = rawData;
		rawData = new Object[size - 1];
		for (int i = 0; i < size - 1; i++) {
			rawData[i] = temp[i+1];
		}
		size--;
		
		return del_value;
	}
	
	/*
	 * deleteRear(E obj)
	 * First check the array is empty. When array is empty, throw exception => constant time. 
	 * When the array has elements already, just delete rawData[size - 1] => constant time.
	 * So time complexity is constant time
	 * O(1)
	 */
	public E deleteRear() {
		if (size <= 0) {
			return (E) "deleting the rear item is not possible.";
		}
		E del_value = getRear();
		rawData[size - 1] = null;
		size--;
		return del_value;
	}
	
	public E getFront() {
		if (size <= 0) {
			return (E) "There is no element.";
		}
		return (E) rawData[0];
	}
		
	public E getRear() {
		if (size <= 0) {
			return (E) "There is no element.";
		}
		return (E) rawData[size - 1];
	}
	
	public int size() {
		return size;
	}
	
	public E get(int index) {
		return (E) rawData[index];
	}
	/*
	 * TODO: Implement getFront(), getRear(), and size() methods.
	 * I'm not providing the skeleton for these, so you'll have to come up with your own.
	 */
	
	public static void main(String[] args) {
		HW3<Integer> hw = new HW3<Integer>();
		hw.insertFront(1);
		hw.insertFront(2);
		hw.insertFront(3);
		hw.insertFront(4);
		hw.insertRear(14);
		hw.insertFront(1314);

		for (int i = 0; i < hw.size(); i++) {
			System.out.print(hw.rawData[i] + " ");
		}
		System.out.println();
		System.out.println(hw.size());
		System.out.println(hw.deleteFront());
		System.out.println(hw.deleteRear());
		
		for (int i = 0; i < hw.size(); i++) {
			System.out.print(hw.rawData[i] + " ");
		}
	}
}