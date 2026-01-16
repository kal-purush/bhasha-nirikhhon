package internet.market;

import internet.market.dao.jdbc.ProductDaoJdbcImpl;
import internet.market.model.Product;

public class Main {

    public static void main(String[] args) {
        ProductDaoJdbcImpl productDaoJdbc = new ProductDaoJdbcImpl();
        Product product1 = new Product("mediator", 100);
        productDaoJdbc.create(product1);
        System.out.println(productDaoJdbc.get(1L));
        Product product2 = new Product("aF", 234);
        product2.setId(1L);
        System.out.println(productDaoJdbc.update(product2));
        productDaoJdbc.delete(1L);
        System.out.println(productDaoJdbc.getAll());
    }
}