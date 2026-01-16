package oos_template_method_pattern;

/*
 * use the template method to create the chickenteriyakisandwhich
 */
public class ChickenTeriyakiSandwichConcreteClass extends SubwaySandwichAbstractClass{

	String meatUsed[] = {"sweetLittleYellowChickensTurnRed"};
	String cheeseUsed[] = {"cheddar","gouda","emmentaler"};
	String veggiesUsed[] = {"jalapenos","green pepper","olives"};
	
	/*
	 * override our forced abstract methods with the ingredients of our 
	 * tasty sandwich
	 */
	
	@Override
	void addMeat() {
		String out="";
		for(String item : meatUsed) {
			out+=item+" ";
		}
		System.out.println("Added: "+out.substring(0,out.length()-1));
	}

	@Override
	void addCheese() {
		String out="";
		for(String item : cheeseUsed) {
			out+=item+" ";
		}
		System.out.println("Added: "+out.substring(0,out.length()-1));
	}

	@Override
	void addVegetables() {
		String out="";
		for(String item : veggiesUsed) {
			out+=item+" ";
		}
		System.out.println("Added: "+out.substring(0,out.length()-1));
	}

}