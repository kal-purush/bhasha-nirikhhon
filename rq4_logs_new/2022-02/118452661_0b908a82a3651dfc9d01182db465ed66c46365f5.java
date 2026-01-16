package concepts.clone;

public class Clone implements Cloneable{
	
	protected Object clone() throws CloneNotSupportedException {
		System.out.println("Cloner.clone()");
        return super.clone();
    }

}