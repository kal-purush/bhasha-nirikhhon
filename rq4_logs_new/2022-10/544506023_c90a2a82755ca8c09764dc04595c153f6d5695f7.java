package bankingApplication;

public class BankDetails {
	

		private String accountNumber;
		private int pinNumber;
		private double balance;

		public BankDetails(String accountNumber, int pinNumber, double balance) {

			this.accountNumber = accountNumber;
			this.pinNumber = pinNumber;
			this.balance = balance;
		}

		public String getAccountNumber() {
			return accountNumber;
		}

		public void setBalance(double balance) {
			this.balance = balance;
		}

		// withdraw and update balance
		void updateBalance(double transactionAmount, int pin) {
			if (pin == pinNumber) {
				if (balance > transactionAmount) {
					balance -= transactionAmount;
				} else {
					System.out.println("Balance not enough");
				}
			} else {
				System.out.println("PIN is incorrect");
			}
		}

		void getBalance() {
			System.out.println("Balance: " + balance);
		}

//		public int getPinNumber() {
//			return pinNumber;
//		}

		public void setPinNumber(int currentPIN, int newPIN) {
			if (this.pinNumber == currentPIN) {
				this.pinNumber = newPIN;
			} else {
				System.out.println("You have entered wrong PIN");
			}
		}

	}