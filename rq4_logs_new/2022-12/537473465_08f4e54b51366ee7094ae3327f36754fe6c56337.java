package group4.householdhero.model;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import group4.householdhero.view.App;

public class CategoryLocaliser {
	private Model model;
	private Localiser localiser;
	private List<String> categoriesInDatabase;
	private List<String> localisedCategories;
	
	
	public CategoryLocaliser(Model model, Localiser localiser) throws SQLException {
		this.model = model;
		this.localiser = localiser;
		categoriesInDatabase = model.getCategories();
	}
	
	/**
	 * Updates the language of categories
	 */
	public void localizeCategories() {
		localisedCategories = new ArrayList<String>();
		
		for (String category : categoriesInDatabase) {
			localisedCategories.add(localiser.getLocalisedString(category));
		}
	}
	
	/**
	 * Returns a localized category to it's original form
	 * @param localizedCategory
	 * @return
	 */
	public String unlocaliseCategory(String localizedCategory) {
		return categoriesInDatabase.get(localisedCategories.indexOf(localizedCategory));
	}
	
	/**
	 * Localizes the categories of a list of products
	 * @param products
	 * @return
	 */
	public List<Product> localise(List<Product> products) {
		List<Product> localizedProducts = new ArrayList<Product>();
		
		for (Product product : products) {
			product.setLocalizedCategory(localiser.getLocalisedString(product.getCategory()));
		}
		
		return localizedProducts;
	}
	
	public List<String> getLocalizedCategories() {
		return localisedCategories;
	}
	
	
}