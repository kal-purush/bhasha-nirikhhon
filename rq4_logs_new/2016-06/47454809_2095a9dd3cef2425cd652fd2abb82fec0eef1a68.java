package models;

import javax.persistence.Column;

public class Counter {

	@Column
	private int id;
	@Column
	private int counter;
	
	public Counter(){
		super();
	}

	public Counter(int id, int counter) {
		super();
		this.id = id;
		this.counter = counter;
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getCounter() {
		return counter;
	}
	public void setCounter(int counter) {
		this.counter = counter;
	}
}