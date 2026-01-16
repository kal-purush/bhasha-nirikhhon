package input.model;

public class Thing
{
	private String name;
	private int age;
	private double weight;
	
	public Thing()
	{
		age = -99;
		weight = -.00005;
		name = "";
	}
	
	public Thing(String name, int age, double weight)
	{
		this.age = age;
		this.name = name;
		this.weight = weight;
	}
	
}