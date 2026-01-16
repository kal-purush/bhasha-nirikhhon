package hal_shop.util.dao.category;

import java.util.List;

import hal_shop.util.dao.DBManager;
import hal_shop.util.dao.product.ProductDTO;
import hal_shop.util.dao.product.ProductMapping;

public class CategoryDAO {
	public List<CategoryDTO> getall() {
		DBManager dbm = new DBManager();
		List<CategoryDTO> dto = dbm.get(new CategoryMapping());
		dbm.close();
		return dto;
	}
	
	public CategoryDTO find(String key) {
		DBManager dbm = new DBManager();
		CategoryDTO dto = dbm.find(key, new CategoryMapping());
		dbm.close();
		return dto;
	}
	
	public List<ProductDTO> getProducts(String categoryKey) {
		DBManager dbm = new DBManager();
		List<ProductDTO> dto = dbm.hasMany(categoryKey, new CategoryMapping(), new ProductMapping());
		dbm.close();
		return dto;
	}
	
}