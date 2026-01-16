package examples.templateDecorator;

public class CoffeeStoreMain {

	public static void main(String[] args) {
		
		Beverage beverage = new Espresso();

		System.out.println(beverage.getDescription()+" cost : "+beverage.cost());

		Beverage beverage1 = new DarkRoast(); // cost 4000

		beverage1 = new Mocha(beverage1);  // mocha cost 500

		beverage1 = new Mocha(beverage1);

		beverage1 = new Whip(beverage1);  // whip cost 5000

		System.out.println(beverage1.getDescription()+" cost : "+beverage1.cost());

		

		Beverage beverage2 = new HouseBlend(); // 2500

		beverage2 = new Soy(beverage2);  // 500

		beverage2 = new Mocha(beverage2);

		beverage2 = new Whip(beverage2);

		System.out.println(beverage2.getDescription()+" cost : "+beverage2.cost());



	}

}