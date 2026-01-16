package beans;

public class Address {
	
	private String street;
	private String number;
	private String city;
	private int postal;
	
	public Address() {}
	
	
	
	public Address(String street, String number, String city, int postal) {
		super();
		this.street = street;
		this.number = number;
		this.city = city;
		this.postal = postal;
	}



	public String getStreet() {
		return street;
	}
	public void setStreet(String street) {
		this.street = street;
	}
	public String getNumber() {
		return number;
	}
	public void setNumber(String number) {
		this.number = number;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public int getPostal() {
		return postal;
	}
	public void setPostal(int postal) {
		this.postal = postal;
	}
	
	
}