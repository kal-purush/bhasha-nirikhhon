package Child;

import Parent.Parent;

public class Child extends Parent {
	public int standard;
	public Child(){
		super();
		standard = 777;
	}
	public int protectParent(){
		return protect;
	}
	/* Becase abc is private in super class a getter method cannot access it in the child.
	public int getter() {
		return abc;
	}
	*/ 

}