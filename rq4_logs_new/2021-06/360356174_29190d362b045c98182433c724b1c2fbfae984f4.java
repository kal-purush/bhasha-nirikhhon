package banking_system;

public class Account {
	
	private int account_no;
	private String first_name;
	private String last_name;
	private double balance;
	
	public Account(int account_no, String first_name, String last_name, double balance) {
		super();
		this.account_no = account_no;
		this.first_name = first_name;
		this.last_name = last_name;
		this.balance = balance;
	}

	public int getAccount_no() {
		return account_no;
	}

	public void setAccount_no(int account_no) {
		this.account_no = account_no;
	}

	public String getFirst_name() {
		return first_name;
	}

	public void setFirst_name(String first_name) {
		this.first_name = first_name;
	}

	public String getLast_name() {
		return last_name;
	}

	public void setLast_name(String last_name) {
		this.last_name = last_name;
	}

	public double getBalance() {
		return balance;
	}

	public void setBalance(double balance) {
		this.balance = balance;
	}

	@Override
	public String toString() {
		return "Account [account_no=" + account_no + ", first_name=" + first_name + ", last_name=" + last_name
				+ ", balance=" + balance + "]";
	}
	
	 

}