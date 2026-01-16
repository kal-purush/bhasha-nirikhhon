package com.badlogic.gdx.math;

public interface Matrix<T> {
	public String toString ();
	
	public float det ();
	
	public float[] getValues ();
	
	public T idt ();
	
	public T mul (T t);
	
	public T mulLeft (T m);
	
	public T inv ();
	
	public T set (T mat);
	
	public T set (float[] values);

}