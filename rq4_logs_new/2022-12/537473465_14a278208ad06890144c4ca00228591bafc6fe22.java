package group4.householdhero.model;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import group4.householdhero.view.App;

public class CategoryLocalizer {
	private Model model;
	private List<String> categoriesInDatabase;
	List<String> localizedCategories;
	
	public CategoryLocalizer(Model model) throws SQLException {
		this.model = model;
		categoriesInDatabase = model.getCategories();
	}
	
	public void localizeCategories() {
		localizedCategories = new ArrayList<String>();
		
		for (String category : categoriesInDatabase) {
			localizedCategories.add(App.bundle.getString(category));
		}
	}
	
	public String unlocalizeCategory(String localizedCategory) {
		return categoriesInDatabase.get(localizedCategories.indexOf(localizedCategory));
	}
	
	public List<String> getLocalizedCategories() {
		return localizedCategories;
	}
	
	public List<Product> localize(List<Product> products) {
		List<Product> localizedProducts = new ArrayList<Product>();
		
		for (Product product : products) {
			product.setCategory(App.bundle.getString(product.getCategory()));
		}
		
		return localizedProducts;
	}
}