package com.internet.shop.dao.jdbc;

import com.internet.shop.dao.ProductDao;
import com.internet.shop.exceptions.DataProcessingException;
import com.internet.shop.lib.Dao;
import com.internet.shop.model.Product;
import com.internet.shop.util.ConnectionUtil;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Dao
public class ProductDaoJdbcImpl implements ProductDao {
    @Override
    public Product create(Product product) {
        String query = "INSERT INTO products (name, price) VALUES (?, ?)";
        try (Connection connection = ConnectionUtil.getConnection()) {
            Product newProduct = new Product(product.getName(), product.getPrice());
            PreparedStatement statement = connection
                    .prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            statement.setString(1, product.getName());
            statement.setDouble(2, product.getPrice());
            statement.executeUpdate();
            ResultSet resultSet = statement.getGeneratedKeys();
            if (resultSet.next()) {
                newProduct.setId(resultSet.getLong(1));
            }
            return newProduct;
        } catch (SQLException e) {
            throw new DataProcessingException("Can't create product", e);
        }
    }

    @Override
    public Product update(Product product) {
        String query = "UPDATE products SET name = ?, "
                + "price = ? WHERE product_id = ? AND deleted = FALSE";
        try (Connection connection = ConnectionUtil.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setString(1, product.getName());
            statement.setDouble(2, product.getPrice());
            statement.setLong(3, product.getId());
            int successfulUpdate = statement.executeUpdate();
            if (successfulUpdate != 0) {
                return product;
            }
            throw new SQLException();
        } catch (SQLException e) {
            throw new DataProcessingException("Can`t update product", e);
        }
    }

    @Override
    public Optional<Product> getById(Long productId) {
        String query = "SELECT * FROM products WHERE product_id = ? AND deleted = FALSE";
        try (Connection connection = ConnectionUtil.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setLong(1, productId);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                Product product = getProductFromResultSet(resultSet);
                return Optional.of(product);
            }
            return Optional.empty();
        } catch (SQLException e) {
            throw new DataProcessingException("Can't get product by id", e);
        }
    }

    @Override
    public List<Product> getAll() {
        String query = "SELECT * FROM products WHERE deleted = FALSE";
        List<Product> productList = new ArrayList<>();
        try (Connection connection = ConnectionUtil.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            ResultSet resultSet = preparedStatement.executeQuery(query);
            while (resultSet.next()) {
                long productId = resultSet.getLong("product_id");
                Product product = getProductFromResultSet(resultSet);
                product.setId(productId);
                productList.add(product);
            }
        } catch (SQLException e) {
            throw new DataProcessingException("Incorrect getAll query", e);
        }
        return productList;
    }

    @Override
    public boolean delete(Long productId) {
        try (Connection connection = ConnectionUtil.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(
                    "UPDATE products SET deleted = TRUE "
                            + "WHERE product_id = ?");
            statement.setLong(1, productId);
            return statement.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new DataProcessingException("Deleting product with id " + productId
                    + " failed", e);
        }
    }

    private Product getProductFromResultSet(ResultSet resultSet) {
        try {
            long productId = resultSet.getLong("product_id");
            String name = resultSet.getString("name");
            double price = resultSet.getDouble("price");
            Product product = new Product(name, price);
            product.setId(productId);
            return product;
        } catch (SQLException e) {
            throw new DataProcessingException("Can't get product from resultSet", e);
        }
    }
}