package com.ocajp.inheritence;

public abstract class AbstractTest {

	/*
	 *You can never create objects of an
class.
abstract
A base class can be defined as an
class, even if it doesn�t define any
abstract
�
methods.
abstract
A derived class should implement all the
methods of its base class. If it
abstract
�
doesn�t, it must be defined as an
derived class.
abstract
You can use variables of an
base class to refer to objects of its derived
abstract
� */
	
	 void println() {};//can have method with body and overriden in child
	 
	protected abstract void dsiplay();//can have method abstract and overriden in child
}


class InheritAbstract extends AbstractTest{

	@Override
	void println() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void dsiplay() {
		// TODO Auto-generated method stub
		
	}
	
	
}