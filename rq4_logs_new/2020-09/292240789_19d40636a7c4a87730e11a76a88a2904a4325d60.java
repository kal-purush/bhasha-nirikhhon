package internet.market.dao;

import internet.market.db.Storage;
import internet.market.lib.Dao;
import internet.market.model.Product;
import java.util.List;
import java.util.Optional;

@Dao
public class ProductDaoImpl implements ProductDao {

    @Override
    public Product create(Product product) {
        Storage.addProduct(product);
        return new Product(product.getName(), product.getPrice());
    }

    @Override
    public Optional<Product> get(Long id) {
        return Storage.products.stream()
                .filter(product -> product.getId().equals(id))
                .findFirst();
    }

    @Override
    public List<Product> getAll() {
        return Storage.products;
    }

    @Override
    public Product update(Product product) {
        for (Product productUnit : Storage.products) {
            if (productUnit.getId().equals(product.getId())) {
                productUnit.setName(product.getName());
                productUnit.setPrice(product.getPrice());
            }
        }
        return product;
    }

    @Override
    public boolean delete(Long id) {
        for (Product product : Storage.products) {
            if (product.getId().equals(id)) {
                Storage.products.remove(product);
                return true;
            }
        }
        return false;
    }
}