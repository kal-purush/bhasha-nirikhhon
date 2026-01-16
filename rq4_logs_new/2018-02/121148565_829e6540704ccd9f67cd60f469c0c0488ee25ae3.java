/**
 * Representative Class of Vending Machine of Drink
 * 
 */
package net.arachmat.java.exercise;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author arachmat
 *
 */
public class VendingMachine {
	
	private Map<String,BigDecimal> catalogueDrink;
	
	
	/**
	 * 
	 */
	public VendingMachine(){
		catalogueDrink = new HashMap<>();
		catalogueDrink.put("Aqua", new BigDecimal(3000));
		catalogueDrink.put("Milo", new BigDecimal(5000));
		catalogueDrink.put("Sosro", new BigDecimal(5000));
		
	}
	
	
	/**
	 * 
	 * @param drinkName
	 * @return
	 */
	private String drinkDispenser(int drinkName) {
		
		return null;
	}
	
	
	/**
	 * 
	 * @param drinkButton
	 * @return
	 */
	private String drinkCatalogue(int drinkButton) {
		
		return null;
	}
	
	
	/**
	 * 
	 * @return
	 */
	public List<String> getAvailableCatalogue() {
		Object[] listCatalogue = this.catalogueDrink.keySet().toArray();
		List<String> finalCatalogue = new ArrayList<String>();
		
		for(int i=0; i<listCatalogue.length; i++) {
			finalCatalogue.add((String)listCatalogue[i]);
		}
		return finalCatalogue;
	}
	
	
	/**
	 * 
	 * @param drinkName
	 * @return
	 */
	public Integer getDrinkPrice(String drinkName) {
		return this.catalogueDrink.get(drinkName).intValue();
	}
	
	
	/**
	 * 
	 * @param drinkName
	 * @param cashMoney
	 * @return
	 */
	public Integer calculateMoneyChange(String drinkName, Integer cashMoney) {
		Integer drinkPrice = this.getDrinkPrice(drinkName);
		
		return cashMoney - drinkPrice;
	}
}