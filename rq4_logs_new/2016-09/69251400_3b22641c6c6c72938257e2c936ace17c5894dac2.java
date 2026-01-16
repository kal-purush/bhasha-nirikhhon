package Air.c.in;

public class AirDemo {

	public static void main(String[] args) {
		AirportC obj = new AirportC("Sujay", 12151, 2016, 9008363220l, true, 28, true);
		obj.checkWeight(obj);
		obj.checkExpiry(obj);
		obj.checkPurchase(obj);
		obj.display(obj);
	}

}