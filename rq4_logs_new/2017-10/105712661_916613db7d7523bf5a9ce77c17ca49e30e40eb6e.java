package DesignPattern;



public abstract class Computer {
	
	public abstract String getRAM();

	/*
	toString: public String toString()

	Returns a string representation of the object. In general, the toString method returns a string that "textually represents" this object. 

	Returns:a string representation of the object.

	 */
	
	@Override
	public String toString(){
		return "RAM= "+this.getRAM();
	}
}