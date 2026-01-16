package examples;

import java.util.ArrayList;

public class MyStack<E> implements Stack<E> {

	private E[] stor = (E[]) new Object[15];// = new E[];
	int stackPtr; // points to the next free position in stor
	
	@Override
	public void push(E o) {
		stor[stackPtr++]=o;
	}

	@Override
	public E pop() {
		return stor[--stackPtr];
	}

	@Override
	public E top() {
		return stor[stackPtr-1];
	}

	@Override
	public int size() {
		return stackPtr;
	}

	@Override
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return stackPtr==0;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}