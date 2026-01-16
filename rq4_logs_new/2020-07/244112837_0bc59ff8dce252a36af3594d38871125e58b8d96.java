package practices.java;

import practices.samples.TestInterface;

public class AnotherTestImplements implements TestInterface {

	@Override
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getZero() {
		// TODO Auto-generated method stub
		return TestInterface.super.getZero();
	}

}