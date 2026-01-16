package arrayvisitors.adt;

import java.io.IOException;

import arrayvisitors.util.MyLogger;
import arrayvisitors.util.MyLogger.DebugLevel;
import arrayvisitors.validations.UndefinedBehaviourException;
import arrayvisitors.visitors.Visitor;

public class MyArray implements MyArrayI{
	
	private int size=10,currentLen=0;
	private int origArr[] = new int[10];
	private DebugLevel debugLevel;
	
	/**
	 * MyArray constructor calls myLogger
	 *
	 * @return 	NULL
	 * 
	 */
	public MyArray() {
		this.debugLevel = DebugLevel.MY_ARRAY;
		MyLogger.writeMessage("in MyArray class empty constructor", debugLevel);
	}
	
	
	/**
	 * MyArray constructor stores given int array
	 *
	 * @return 	NULL
	 * 
	 */
	public MyArray(int[] val) {
		origArr = val;
	}
	
	
	/**
	 * get size of int[] of a given MyArray Object
	 *
	 * @return	int	Size
	 * 
	 */
	public int getSize() {
		return size;
	}
	
	
	/**
	 * increases size int[] of given MyArray Object when array is full
	 *
	 * @return	NULL
	 * 
	 */
	public void setSize() {
		size+=(size/2);
	}
	
	
	/**
	 * get currentLen of given MyArray Object
	 *
	 * @return	int	currentLen
	 * 
	 */
	public int getCurrentLen() {
		return currentLen;
	}
	
	
	/**
	 * increases currentLen of int[] of given MyArray Object 
	 * when new item is added
	 *
	 * @return	NULL
	 * 
	 */
	public void setCurrentLen() {
		currentLen += 1;
	}
	
	
	/**
	 * get int[] array of given MyArray Object
	 *
	 * @return	int[]	origArr
	 * 
	 */
	public int[] getMyArray() {
		return origArr;
	}
	
	
	/**
	 * if array is full then increase the size and add new element 
	 * to given index
	 *
	 * @return	NULL
	 * 
	 */
	public void addElement(int val,int index) {
		if(currentLen == size) {
			int[] resizedArray = new int[size + (size/2)];
			System.arraycopy(origArr, 0, resizedArray, 0, origArr.length);
			origArr = resizedArray;
			setSize();
		}

		origArr[index] = val;
		
		setCurrentLen();
		
	}

	
	/**
	 * accepts appropriate visit method
	 *
	 * @return	NULL
	 * 
	 */
	@Override
	public void accept(Visitor v) {
		// TODO Auto-generated method stub
		try {
			v.visit(this);
		} catch (UndefinedBehaviourException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	
	/**
	 * for better design principles
	 *
	 * @return	NULL
	 * 
	 */
	public void finalize() {
		
	}
	
	
	/**
	 * clone a MyArray
	 *
	 * @return	MyArray Object
	 * 
	 */
	public MyArray clone()
	{
		return new MyArray(this.getMyArray());
	}
	
	
	/**
     * Default toString, needed for debugging here.
     * 
     * @return String
     */
    public String toString() {
        return ("debugLevel: " + debugLevel);
    }
	
}