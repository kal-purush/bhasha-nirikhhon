import java.io.IOException;


public class AccontTest {

	public static void main(String[] args) throws OverdraftException, IOException {
		
		AccountContainer container = new AccountContainer();
		container.read();
		
		//person 1
		// deposit 1, 1000$
		
		Account account = container.getAccount(1);
		if(account == null) {
			account = new Account(1, 0);
			container.addAccount(account);
		}
		account.depositAmount(1000);
		
		// person 2
		// deposit 200$ to his account 2
		account = container.getAccount(2);
		if(account == null) {
			account = new Account(2, 0);
			container.addAccount(account);
		}
		account.depositAmount(200);
		
		// person 1
		// withdraw 200$
		account = container.getAccount(1);
		account.drawAmount(200);
		
		container.printAllAccounts();
		container.save();
	}

}