package group4.householdhero.model;

import java.time.LocalDate;

public class ProductFactory {
	
	/**
	 * Creates a new Product-object
	 * @param id
	 * @param name
	 * @param price
	 * @param bestBefore
	 * @param category
	 * @param budgetId
	 * @param statusId
	 * @param model
	 * @return
	 */
	public Product createProduct(int id, String name, double price, LocalDate bestBefore, String category, int budgetId, int statusId, Model model) {
		return new Product(id, name, price, bestBefore, category, budgetId, statusId, model);
	}
}