package com.sweng894.GetVaccinated;

public class Person {

	private String name;
	private String phase;
	private String isVaccinated;

	public Person() {

	}

	public Person(String name, String phase, String isVaccinated) {
		this.name = name;
		this.phase = phase;
		this.isVaccinated = isVaccinated;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPhase() {
		return phase;
	}

	public void setPhase(String phase) {
		this.phase = phase;
	}

	public String getIsVaccinated() {
		return isVaccinated;
	}

	public void setIsVaccinated(String isVaccinated) {
		this.isVaccinated = isVaccinated;
	}

}