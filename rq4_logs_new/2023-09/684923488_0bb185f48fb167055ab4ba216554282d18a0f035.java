package ru.aston.tarakanov_aa.task4.Dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.ArrayList;

import ru.aston.tarakanov_aa.task4.Connection.ConnectorToH2Base;
import ru.aston.tarakanov_aa.task4.Entity.Order;

public class OrderDao implements DaoDataEntityLayer<Order>, AutoCloseable{
    private static final String SQL_SELECT_ALL_ORDERS = "SELECT * FROM orders";
    private static final String SQL_SELECT_ORDER_BY_ID = "SELECT * FROM orders WHERE id=?";
    private static final String SQL_DELETE_ORDER_BY_ID = "DELETE FROM orders WHERE id=?";
    private static final String SQL_CREATE_ORDER = "INSERT INTO orders (product, weight, price)" + 
    											   								"VALUES (?,?,?)";
    private static final String SQL_UPDATE_ORDER_BY_ID = "UPDATE orders SET product = ?, weight = ?," +
    											   						" price = ? WHERE id = ?";
    
    private Connection connection;
    
    public OrderDao() throws SQLException {
			this.connection = ConnectorToH2Base.getConnection();
    }
    
	@Override
	public List<Order> findAll() {
		List<Order> orderList = new ArrayList<>();
		try (Statement statement = connection.createStatement()){
			ResultSet resultSet = statement.executeQuery(SQL_SELECT_ALL_ORDERS);
			
			while (resultSet.next()){
				orderList.add(new Order(resultSet.getLong(1), resultSet.getString("product"), 
										resultSet.getDouble(3), resultSet.getDouble(4)));
            }
			resultSet.close();
		} catch (SQLException e) {	
			e.printStackTrace();
		}		
		return orderList;
	}
	
	@Override
	public Order findEntityById(long id) {
		 Order order = null;
		 try (PreparedStatement statement = connection.prepareStatement(SQL_SELECT_ORDER_BY_ID)){			 
			 statement.setLong(1, id);
			 ResultSet resultSet = statement.executeQuery();
			 if(resultSet.next()){
	                order = new Order(resultSet.getLong(1), resultSet.getString(2), 
										resultSet.getDouble(3), resultSet.getDouble(4));
	            }
			 resultSet.close();
		 } catch (SQLException e) {	
			 e.printStackTrace();
		 }		
		 
		return order;
	}
	
	@Override
	public boolean delete(long id) {
		boolean isDelete = false;
        try(PreparedStatement statement = connection.prepareStatement(SQL_DELETE_ORDER_BY_ID)) {
            statement.setLong(1, id);
            if (statement.executeUpdate() > 0){
                isDelete = true;
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return isDelete;
	}
	
	@Override
	public boolean create(Order order) {
		boolean isCreate = false;
        try(PreparedStatement statement = connection.prepareStatement(SQL_CREATE_ORDER)) {
            statement.setString(1, order.getProduct());
            statement.setDouble(2, order.getWeight());
            statement.setDouble(3, order.getPrice());
            if (statement.executeUpdate() > 0){
                isCreate = true;
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return isCreate;
	}
	
	@Override
	public Order update(Order order) {
		Order resultOrder = null;
        try(PreparedStatement statement = connection.prepareStatement(SQL_UPDATE_ORDER_BY_ID)) {
            statement.setString(1, order.getProduct());
            statement.setDouble(2, order.getWeight());
            statement.setDouble(3, order.getWeight());
            statement.setLong(4, order.getId());
            if (statement.executeUpdate() > 0){
                resultOrder = findEntityById(order.getId());
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return resultOrder;
	}

	@Override
	public void close() {
		try {
			connection.close();
		} catch (SQLException e) {			
			System.out.println(e.getMessage());
		}		
	}
    
    
}