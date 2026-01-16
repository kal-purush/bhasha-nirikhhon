package com.guideme.pattern;

public class AbstractFactoryPatternExample1 {

	public static void main(String[] args) {
		PersonFactory pf = new PersonFactory();

		PersonDto p1 = pf.createPerson("Chris");
		System.out.println(p1);
		PersonDto p2 = pf.createPerson("Sarah");
		System.out.println(p2);
		PersonDto p3 = pf.createPerson("John");
		System.out.println(p3);
	}

}

class PersonDto {
	public int id;
	public String name;

	public PersonDto(int id, String name) {
		this.id = id;
		this.name = name;
	}

	@Override
	public String toString() {
		return "PersonDto [id=" + id + ", name=" + name + "]";
	}

}

class PersonFactory {
	private int id = 0;

	public PersonDto createPerson(String name) {
		return new PersonDto(id++, name);
	}
}