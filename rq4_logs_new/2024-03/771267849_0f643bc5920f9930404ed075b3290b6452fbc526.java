package MiniProject.Bank;

public class Bank {
	private String account_num;
	private int isAccount;

	public String getAccount_num() {
		return account_num;
	}

	public void setAccount_num(String account_num) {
		this.account_num = account_num;
	}

	public int getIsAccount() {
		return isAccount;
	}

	public void setIsAccount(int isAccount) {
		this.isAccount = isAccount;
	}

	public Bank(String account_num, int isAccount) {
		super();
		this.account_num = account_num;
		this.isAccount = isAccount;
	}

	public Bank() {
		
	}
	@Override
	public String toString() {
		return "BankDao [account_num=" + account_num + ", isAccount=" + isAccount + "]";
	}

}