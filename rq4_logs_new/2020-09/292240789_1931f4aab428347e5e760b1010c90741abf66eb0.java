package internet.market.dao.jdbc;

import internet.market.dao.OrderDao;
import internet.market.exceptions.DataProcessingException;
import internet.market.lib.Dao;
import internet.market.model.Order;
import internet.market.model.Product;
import internet.market.util.ConnectionUtil;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Dao
public class OrderDaoJdbcImpl implements OrderDao {

    @Override
    public List<Order> getUserOrders(Long userId) {
        String query = "SELECT * FROM orders WHERE user_id = ? AND deleted = false";
        List<Order> orderList = new ArrayList<>();
        try (Connection connection = ConnectionUtil.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setLong(1, userId);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                orderList.add(getOrderInformationFromDataBase(resultSet));
            }
        } catch (SQLException e) {
            throw new DataProcessingException("Can`t get orders of user with id = " + userId, e);
        }
        for (Order order : orderList) {
            order.setProducts(getProductsOfOrder(order.getId()));
        }
        return orderList;
    }

    @Override
    public Order create(Order item) {
        String query = "INSERT INTO orders(user_id) VALUES (?)";
        try (Connection connection = ConnectionUtil.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(query,
                    Statement.RETURN_GENERATED_KEYS);
            statement.setLong(1, item.getUserId());
            statement.executeUpdate();
            ResultSet resultSet = statement.getGeneratedKeys();
            if (resultSet.next()) {
                item.setId(resultSet.getLong(1));
            }
        } catch (SQLException e) {
            throw new DataProcessingException("Can`t add order with id =  " + item.getId(), e);
        }
        addProductsToOrder(item.getProducts(), item.getId());
        return item;
    }

    @Override
    public Optional<Order> get(Long id) {
        String query = "SELECT * FROM orders WHERE order_id = ? AND deleted = false";
        Order order = null;
        try (Connection connection = ConnectionUtil.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setLong(1, id);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                order = getOrderInformationFromDataBase(resultSet);
                order.setProducts(getProductsOfOrder(id));
                return Optional.of(order);
            }
        } catch (SQLException e) {
            throw new DataProcessingException("Can`t find order with id = "
                    + id, e);
        }
        if (order != null) {
            order.setProducts(getProductsOfOrder(id));
        }
        return Optional.ofNullable(order);
    }

    @Override
    public List<Order> getAll() {
        List<Order> allOrders = new ArrayList<>();
        String query = "SELECT * FROM orders WHERE deleted = false";
        try (Connection connection = ConnectionUtil.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(query);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                allOrders.add(getOrderInformationFromDataBase(resultSet));
            }
        } catch (SQLException e) {
            throw new DataProcessingException("Can't get all orders", e);
        }
        for (Order order : allOrders) {
            order.setProducts(getProductsOfOrder(order.getId()));
        }
        return allOrders;
    }

    @Override
    public Order update(Order item) {
        String updateOrderQuery = "UPDATE orders SET user_id = ? "
                + "WHERE order_id = ? AND deleted = false";
        try (Connection connection = ConnectionUtil.getConnection()) {
            PreparedStatement statement =
                    connection.prepareStatement(updateOrderQuery);
            statement.setLong(1, item.getUserId());
            statement.setLong(2, item.getId());
            statement.executeUpdate();
            List<Product> allProducts = getProductsOfOrder(item.getId());
            deleteProductsFromOrder(item.getId());
            addProductsToOrder(allProducts, item.getId());
        } catch (SQLException e) {
            throw new DataProcessingException("Can't update order with id = " + item.getId(), e);
        }
        return item;
    }

    @Override
    public boolean delete(Long id) {
        String query = "UPDATE orders SET deleted = true WHERE order_id = ?";
        try (Connection connection = ConnectionUtil.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setLong(1, id);
            return preparedStatement.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new DataProcessingException("Can't delete order with id = "
                    + id, e);
        }
    }

    private Order getOrderInformationFromDataBase(ResultSet resultSet)
            throws SQLException {
        Long orderId = resultSet.getLong("order_id");
        Long userId = resultSet.getLong("user_id");
        Order order = new Order(userId);
        order.setId(orderId);
        return order;
    }

    private List<Product> getProductsOfOrder(Long orderId) {
        List<Product> products = new ArrayList<>();
        String query = "SELECT * FROM products JOIN orders_products  "
                + "ON products.product_id = orders_products.product_id"
                + " WHERE orders_products.order_id = ?";
        try (Connection connection = ConnectionUtil.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setLong(1, orderId);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                Long productId = resultSet.getLong("id");
                String productName = resultSet.getString("name");
                double productPrice = resultSet.getDouble("price");
                Product product = new Product(productName, productPrice);
                product.setId(productId);
                products.add(product);
            }
            return products;
        } catch (SQLException e) {
            throw new DataProcessingException("Can't get products from order with id = "
                    + orderId, e);
        }
    }

    private void addProductsToOrder(List<Product> products, long orderId) {
        String query = "INSERT INTO orders_products (order_id, product_id) VALUES (?, ?)";
        try (Connection connection = ConnectionUtil.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(query);
            for (Product product : products) {
                statement.setLong(1, orderId);
                statement.setLong(2, product.getId());
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            throw new DataProcessingException("Can't add products to order with id = "
                    + orderId, e);
        }
    }

    private void deleteProductsFromOrder(Long orderId) {
        String query = "DELETE FROM orders_products WHERE order_id = ?";
        try (Connection connection = ConnectionUtil.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setLong(1, orderId);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new DataProcessingException("Can't delete products from order with id = "
                    + orderId, e);
        }
    }
}