
public class Account {
	
	private String firstName;
	private String lastName;
	private int accountNumber;
	
	public Account(String fName, String sName, int accountNo) {
		this.firstName = fName;
		this.lastName = sName;
		this.accountNumber = accountNo;
	}
	
	@Override
	public String toString() {
		return "Name: " + firstName + " " + lastName + " Account Number: "+ accountNumber; 
	}
	
	public String getfName() {
		return firstName;
	}
	
	public String getsName() {
		return lastName;
	}
	public int getAccountNumber() {
		return accountNumber;
	}
	public void setAccountNumber(int accountNumber) {
		this.accountNumber = accountNumber;
	}
	
}