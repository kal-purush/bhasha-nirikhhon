package inheritance;

class Customer
{
	int acc;
	String name;
	String add;
	
	void setDetails(int x, String y, String z)
	{
	this.acc= x;
	this.name=y;
	this.add=z;
	}
	void getDetails()
	{
		System.out.println("Account no : "+acc);
		System.out.println("Customer Name : "+name);
		System.out.println("Customer Address: "+add);
	}
}
class CreditCard extends Customer
{
	int cardno;
	
	void setCard(int num)
	{
		this.cardno=num;
	}
	void getCard()
	{
		System.out.println(name+"Card Number  = "+cardno);
	}
}
public class Bank {

	public static void main(String[] args) {
		
		CreditCard c1 = new CreditCard();
		c1.setDetails(1010,"naresh","ohio");
		c1.setCard(10000);
		c1.getDetails();
		c1.getCard();
	}

}