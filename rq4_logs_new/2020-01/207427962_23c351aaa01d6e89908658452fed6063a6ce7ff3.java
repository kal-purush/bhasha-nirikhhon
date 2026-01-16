
public class CashRegister {

	private double purchase;
	private double payment;
	
	public CashRegister()
	{
		purchase =0;
		payment =0;
	}
	public void recordPurchase(double amount)
	{
		purchase = purchase+amount;
	}
	void receivePayment(double moneyGiven)
	{
		payment = payment+moneyGiven;
	}
	public double giveChange()
	{
		return (payment-purchase);
	}
	public void printChange() {
		System.out.printf("Your change is: %.2f", giveChange());
	}
	
}
/**
•	REVIEW Assignment for CS176
•	Create a class called CashRegister.
•	CashRegister will use two instance variables:
purchase (double) will be a total of all purchases made by the customer
payment (double) will be the amount the customer is submitting to pay for purchases.
•	CashRegister methods:
o	Constructor will create a cash register with purchase = 0 and payment = 0.
o	recordPurchase will record a sale and add the new sale amount to purchase
o	receivePayment will receive the amount paid by the customer into payment
o	giveChange will calculate and return the amount of change due to the customer (purchase – payment)
•	Create a tester called CashRegisterTester which will:
o	Create a CashRegister object
o	Use a loop to accept purchases from the screen until a SENTINEL value of -1 is typed
o	Accept the payment for the purchases
o	Calculate and print the amount of change due to the customer
•	Document all components of the CashRegister class
•	Run CashRegisterTester several times to achieve the results shown below
*/