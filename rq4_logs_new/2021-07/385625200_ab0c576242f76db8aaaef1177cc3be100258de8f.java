package ro.ase.acs.classes;

import java.io.Serializable;
import java.util.Comparator;

public class Ingredient implements Cloneable, Serializable, Comparable<Ingredient>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * 
	 */
	
	String name = "";
	int quantity = 0;
	float price = 0;

	public Ingredient(String name, int quantity, float price) {
		super();
		this.name = name;
		this.quantity = quantity;
		this.price = price;
	}

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getQuantity() {
		return this.quantity;
	}

	public void setQuantity(int quantity) {
		this.quantity = quantity;
	}

	public float getPrice() {
		return this.price;
	}

	public void setPrice(float price) {
		this.price = price;
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		Ingredient copy = (Ingredient) super.clone();
		copy.name = this.name;
		copy.quantity = this.quantity;
		copy.price = this.price;
		return copy;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Ingredient name: ");
		builder.append(this.name);
		builder.append(" Quantity: ");
		builder.append(this.quantity);
		builder.append(" Price: ");
		builder.append(this.name);
		return builder.toString();
	}

	@Override
	public int compareTo(Ingredient o) {
		// TODO Auto-generated method stub
		return 0;
	}

	
	
	

}










