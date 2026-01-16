package com.internet.shop.dao.jdbc;

import com.internet.shop.dao.ShoppingCartDao;
import com.internet.shop.exceptions.DataProcessingException;
import com.internet.shop.lib.Dao;
import com.internet.shop.model.Product;
import com.internet.shop.model.Role;
import com.internet.shop.model.ShoppingCart;
import com.internet.shop.model.User;
import com.internet.shop.util.ConnectionUtil;

import java.sql.*;
import java.util.*;

@Dao
public class ShoppingCartDaoJdbcImpl implements ShoppingCartDao {
    @Override
    public Optional<ShoppingCart> getByUserId(Long userId) {
        String query = "SELECT * FROM shopping_carts "
                + " WHERE user_id = ? AND deleted = FALSE";
        try (Connection connection = ConnectionUtil.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setLong(1, userId);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                ShoppingCart shoppingCart = getShoppingCartFromResultSet(resultSet);
                shoppingCart.setId(getCartIdByUserId(userId));
                shoppingCart.setProducts(getProductToShoppingCartFromDB(shoppingCart.getId()));
                return Optional.of(shoppingCart);
            }
            return Optional.empty();
        } catch (SQLException e) {
            throw new DataProcessingException("Getting shopping cart by user id "
                    + userId + " failed. ", e);
        }
    }

    @Override
    public ShoppingCart create(ShoppingCart shoppingCart) {
        String query = "INSERT INTO shopping_carts (user_id) VALUES (?)";
        try (Connection connection = ConnectionUtil.getConnection()) {
            ShoppingCart newShoppingCart = new ShoppingCart(shoppingCart.getUserId());
            PreparedStatement statement = connection
                    .prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            statement.setLong(1, shoppingCart.getUserId());
            statement.executeUpdate();
            ResultSet resultSet = statement.getGeneratedKeys();
            if (resultSet.next()) {
                newShoppingCart.setId(resultSet.getLong(1));
            }
            return newShoppingCart;
        } catch (SQLException e) {
            throw new DataProcessingException("Can't create shopping cart with user id "
                    + shoppingCart.getUserId(), e);
        }
    }

    @Override
    public ShoppingCart update(ShoppingCart shoppingCart) {
        String query = "INSERT INTO shopping_carts_products (cart_id, product_id) VALUES (?, ?)";
        Long cart_id = shoppingCart.getId();
        for (Product product : shoppingCart.getProducts()) {
            try (Connection connection = ConnectionUtil.getConnection()) {
                PreparedStatement statement = connection
                        .prepareStatement(query);
                statement.setLong(1, cart_id);
                statement.setLong(2, product.getId());
                statement.executeUpdate();
            } catch (SQLException e) {
                throw new DataProcessingException("Insert to shopping_carts_products with cart_id "
                        + shoppingCart.getId() + "is failed. ");
            }
        }
        return shoppingCart;
    }

    @Override
    public Optional<ShoppingCart> getById(Long cart_id) {
        String query = "SELECT * FROM shopping_carts "
                + " WHERE cart_id = ? AND deleted = FALSE";
        try (Connection connection = ConnectionUtil.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setLong(1, cart_id);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                ShoppingCart shoppingCart = getShoppingCartFromResultSet(resultSet);
                shoppingCart.setProducts(getProductToShoppingCartFromDB(cart_id));
                return Optional.of(shoppingCart);
            }
            return Optional.empty();
        } catch (SQLException e) {
            throw new DataProcessingException("Getting shopping cart by id "
                    + cart_id + " failed. ", e);
        }
    }

    @Override
    public List<ShoppingCart> getAll() {
        String query = "SELECT * FROM shopping_carts "
                + " WHERE deleted = FALSE";
        List<ShoppingCart> shoppingCarts = new ArrayList<>();
        try (Connection connection = ConnectionUtil.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(query);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                ShoppingCart shoppingCart = getShoppingCartFromResultSet(resultSet);
                return shoppingCarts;
            }
            return shoppingCarts;
        } catch (SQLException e) {
            throw new DataProcessingException("Getting all shopping carts from db failed. ", e);
        }
    }

    @Override
    public boolean delete(Long cart_id) {
        try (Connection connection = ConnectionUtil.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(
                    "UPDATE shopping_carts SET deleted = TRUE "
                            + "WHERE cart_id = ?");
            statement.setLong(1, cart_id);
            return statement.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new DataProcessingException("Deleting shopping cart with id " + cart_id
                    + " failed. ", e);
        }
    }

   /* private ShoppingCart bindingShopCartIdWithProduct(ShoppingCart shoppingCart) {
        String query = "INSERT INTO shopping_carts_products (cart_id, product_id) VALUES (?, ?)";
        Long cart_id = shoppingCart.getId();
        for (Product product : shoppingCart.getProducts()) {
            try (Connection connection = ConnectionUtil.getConnection()) {
                PreparedStatement statement = connection
                        .prepareStatement(query);
                statement.setLong(1, cart_id);
                statement.setLong(2, product.getId());
                statement.executeUpdate();
            } catch (SQLException e) {
                throw new DataProcessingException("Insert to shopping_carts_products with cart_id "
                        + shoppingCart.getId() + "is failed. ");
            }
        }
        return shoppingCart;
    }*/

    private ShoppingCart getShoppingCartFromResultSet(ResultSet resultSet) {
        try {
            Long cart_id = resultSet.getLong("cart_id");
            Long user_id = resultSet.getLong("user_id");
            ShoppingCart shoppingCart = new ShoppingCart(user_id);
            shoppingCart.setProducts(getProductToShoppingCartFromDB(cart_id));
            return shoppingCart;
        } catch (SQLException e) {
             throw new DataProcessingException("Can't get product from resultSet", e);
        }
    }

    private Long getCartIdByUserId(Long user_id) {
        String query = "SELECT cart_id FROM shopping_carts WHERE user_id = ?";
        try (Connection connection = ConnectionUtil.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setLong(1, user_id);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                return resultSet.getLong("cart_id");
            }
        } catch (SQLException e) {
            throw new DataProcessingException("Getting shopping cart id by user id "
                    + user_id + " failed. ", e);
        }
        return null;
    }

    private List<Product> getProductToShoppingCartFromDB(Long cart_id) {
        String query = "SELECT shopping_carts_products.product_id "
                + " FROM shopping_carts_products "
                + " JOIN products "
                + " ON shopping_carts_products.product_id = products.product_id "
                + " WHERE shopping_carts_products.cart_id = ? AND shopping_carts_products.deleted = FALSE ";
        List<Product> products = new ArrayList<>();
        try (Connection connection = ConnectionUtil.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setLong(1, cart_id);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                products.add(getProductFromDbById(resultSet.getLong("product_id")));
            }
            return products;
        } catch (SQLException e) {
            throw new DataProcessingException("Can't get products list by cart id "
                    + cart_id, e);
        }
    }

    private Product getProductFromDbById(Long product_id) {
        String query = "SELECT name, price FROM products WHERE product_id = ? AND deleted = FALSE";
        try (Connection connection = ConnectionUtil.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setLong(1, product_id);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                String name = resultSet.getString("name");
                double price = resultSet.getDouble("price");
                Product product = new Product(name, price);
                product.setId(product_id);
                return product;
            }
        } catch (SQLException e) {
            throw new DataProcessingException("Getting product by id "
                    + product_id + " failed. ", e);
        }
        return null;
    }

    @Override
    public boolean clear(ShoppingCart shoppingCart) {
        Long cart_id = shoppingCart.getId();
        for(Product product : shoppingCart.getProducts()) {
            deleteProduct(cart_id, product);
        }
        shoppingCart.getProducts().clear();
        return shoppingCart.getProducts().isEmpty();
    }

    @Override
    public boolean deleteProduct(Long cart_id, Product product) {
        String query = "UPDATE shopping_carts_products SET deleted = TRUE "
                + "WHERE cart_id = ? AND product_id = ?";
        try (Connection connection = ConnectionUtil.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setLong(1, cart_id);
            statement.setLong(2, product.getId());
            return statement.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new DataProcessingException("Deleting shopping cart with id " + cart_id
                    + " failed. ", e);
        }
    }
}