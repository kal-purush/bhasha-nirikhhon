package app;

public class Employee {
	
	// Constructors
	public Employee(int newEmployeeNumber, String newName, String newAddress, String newNIN, String newBankNumber, float newStartingSalary) {
		this(newName, newAddress, newNIN, newBankNumber, newStartingSalary);
		setEmployeeNumber(newEmployeeNumber);
	}
	
	public Employee (String newName, String newAddress, String newNIN, String newBankNumber, float newStartingSalary) {
		this (newAddress, newNIN, newBankNumber, newStartingSalary);
		setName(newName);
	}
	
	public Employee (String newAddress, String newNIN, String newBankNumber, float newStartingSalary) {
		this (newNIN, newBankNumber, newStartingSalary);
		setAddress(newAddress);
	}
	
	public Employee (String newNIN, String newBankNumber, float newStartingSalary) {
		this (newBankNumber, newStartingSalary);
		setNIN(newNIN);
	}
	
	public Employee (String newBankNumber, float newStartingSalary) {
		this (newStartingSalary);
		setBankNumber(newBankNumber);
	}
	
	public Employee (float newStartingSalary) {
		this ();
		setStartingSalary(newStartingSalary);
	}
	
	public Employee () {
		//Finished construct
	}
	
	// Initialisations
	
	private int employeeNumber;
	private String name;
	private String address;
	private String nin;
	private String bankNumber;
	private float startingSalary;
	
	
	//Getters & Setters
	
	public int getEmployeeNumber() {
		return employeeNumber;
	}
	public void setEmployeeNumber(int employeeNumber) {
		this.employeeNumber = employeeNumber;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public String getNIN() {
		return nin;
	}
	public void setNIN(String nin) {
		this.nin = nin;
	}
	public String getBankNumber() {
		return bankNumber;
	}
	public void setBankNumber(String bankNumber) {
		this.bankNumber = bankNumber;
	}
	public float getStartingSalary() {
		return startingSalary;
	}
	public void setStartingSalary(float startingSalary) {
		this.startingSalary = startingSalary;
	}

}