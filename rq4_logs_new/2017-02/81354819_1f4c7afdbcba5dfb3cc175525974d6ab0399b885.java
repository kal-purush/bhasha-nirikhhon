
public class Checking extends Account
{	private double balance;
	private double overdraft_Max;
	public Checking()
	{
		super();
		balance=0.0;
		overdraft_Max=50.0;
		
	}
	public Checking(double d)
	{
		balance=d;
		overdraft_Max=-50.0;
		
	}
	public double getBalance() {
		return balance;
	}
	public void setBalance(double balance) {
		this.balance = balance;
	}
	public double getOverdraft_Max() {
		return overdraft_Max;
	}
	public void setOverdraft_Max(double overdraft_Max) {
		this.overdraft_Max = overdraft_Max;
	}
	
	
}