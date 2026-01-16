package se.kth.iv1201.recruitmentapplicationgroup5.model;

/*
 * Class representing the name of a person.
 */
public class FullName {
	
	private String firstName;
	private String lastName;
	
	public FullName() {
		
	}
	
	public FullName(String firstName, String lastName) {
		this.firstName = firstName;
		this.lastName = lastName;
	}
	
	public String getFirstName() {
		return firstName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	public String getLastName() {
		return lastName;
	}
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	
}