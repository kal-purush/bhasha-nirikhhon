/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package DAOs;

import Models.Cart;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;

/**
 *
 * @author Admin
 */
public class CartDAO {

    private Connection connection;

    public CartDAO(Connection connection) {
        this.connection = connection;
    }

    public Cart getCartById(int cartID) throws SQLException {
        String query = "SELECT * FROM cart WHERE cart_id = ?";
        try ( PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, cartID);
            try ( ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    Cart cart = new Cart();
                    cart.setCart_id(resultSet.getInt("cart_id"));
                    cart.setCus_id(resultSet.getInt("cus_id"));
                    cart.setPro_id(resultSet.getInt("pro_id"));
                    cart.setPro_quantity(resultSet.getInt("pro_quantity"));
                    cart.setCart_price(resultSet.getDouble("cart_price"));
                    return cart;
                }
            }
        }
        return null; // không tìm thấy giỏ hàng với id tương ứng
    }

    public LinkedList<Cart> getAllCarts(int cus_id) throws SQLException {
        LinkedList<Cart> carts = new LinkedList<>();
        String query = "SELECT * FROM cart WHERE cus_id = ?";
        try ( PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, cus_id);
            try ( ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    Cart cart = new Cart();
                    cart.setCart_id(resultSet.getInt("cart_id"));
                    cart.setCus_id(resultSet.getInt("cus_id"));
                    cart.setPro_id(resultSet.getInt("pro_id"));
                    cart.setPro_quantity(resultSet.getInt("pro_quantity"));
                    cart.setCart_price(resultSet.getDouble("cart_price"));
                    carts.add(cart);
                }
            }
        }
        return carts;
    }

    public int editCart(int cartID, Cart cart) throws SQLException {
        String query = "UPDATE cart SET cus_id = ?, pro_id = ?, pro_quantity = ?, cart_price = ? WHERE cart_id = ?";
        try ( PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, cart.getCus_id());
            statement.setInt(2, cart.getPro_id());
            statement.setInt(3, cart.getPro_quantity());
            statement.setDouble(4, cart.getCart_price());
            statement.setInt(5, cartID);
            // statement.executeUpdate();
            int deleteIs = statement.executeUpdate();
            return deleteIs;
        }
    }

    public int deleteCart(int cartID) throws SQLException {
        String query = "DELETE FROM cart WHERE cart_id = ?";
        try ( PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, cartID);
            int deleteIs = statement.executeUpdate();
            return deleteIs;
        }
    }

    public int createCart(Cart cart) throws SQLException {
        String query = "INSERT INTO cart (cart_id, cus_id, pro_id, pro_quantity, cart_price) VALUES (?, ?, ?, ?, ?)";
        try ( PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, cart.getCart_id());
            statement.setInt(2, cart.getCus_id());
            statement.setInt(3, cart.getPro_id());
            statement.setInt(4, cart.getPro_quantity());
            statement.setDouble(5, cart.getCart_price());
            // statement.executeUpdate();
            int deleteIs = statement.executeUpdate();
            return deleteIs;
        }

    }
}