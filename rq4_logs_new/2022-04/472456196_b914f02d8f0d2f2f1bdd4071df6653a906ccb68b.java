package catalog;

import java.util.LinkedList;
import java.util.List;

/**
 * A class that contains a single cart; this cart could contain multiple items from BearNecessities
 * @author naomihorsford, whitney, deyoola
 *
 */

public class Cart {
	
	private List<CatalogItem> cartItems;
	
	public Cart() {
		this.cartItems = new LinkedList<>();
	}
	
	public void addItem(CatalogItem item) {
		cartItems.add(item);
	}
	
	public int getSize() {
		return cartItems.size();
	}
 
	public double computeSubtotal() {
		double sum = 0;
		for(CatalogItem item : cartItems) {
			sum += item.getPrice();
		}
		return sum;
	}
}