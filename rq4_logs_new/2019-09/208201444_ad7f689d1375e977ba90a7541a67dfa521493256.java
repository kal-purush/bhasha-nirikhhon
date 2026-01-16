package com.shoppingcart.dataUtil;

import java.sql.*;
import java.util.*;

import javax.annotation.Resource;
import javax.sql.DataSource;

import com.shoppingcart.entity.Cart;
import com.shoppingcart.entity.CartProduct;
import com.shoppingcart.entity.Product;

public class ShoppingCartUtil {

	@Resource(name = "jdbc/Shopping")
	private DataSource dataSource;

	
	
	public ShoppingCartUtil(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public List<CartProduct> getCartItems(String username) throws Exception {

		List<CartProduct> cartProductList = new ArrayList<>();
		Cart cart;
		Product product = null;
		Connection con = null;
		PreparedStatement pstmt = null;
		PreparedStatement inner_pstmt = null;
		ResultSet rst = null;
		ResultSet inner_rst = null;
		String query;

		try {
			// Connect to DB
			con = dataSource.getConnection();

			// Create query for all cart items of a user
			query = "SELECT * FROM shopping.cart WHERE Username = ?";
			pstmt = con.prepareStatement(query);
			pstmt.setString(1, username);
			// Execute query
			rst = pstmt.executeQuery();
			// Process result set
			while (rst.next()) {
				// Retrieve cart item data from result set
				int productId = rst.getInt("Product_Id");
				int quantity = rst.getInt("Quantity");
				// Create new cart
				cart = new Cart(username, productId, quantity);

				// Create query to get the product of the cart above
				query = "SELECT * FROM shopping.product WHERE Product_Id = ?";
				inner_pstmt = con.prepareStatement(query);
				inner_pstmt.setInt(1, productId);

				// Execute query to get product details
				inner_rst = inner_pstmt.executeQuery();
				if (inner_rst.next()) {
					int product_id = inner_rst.getInt("Product_Id");
					String product_name = inner_rst.getString("Product_Name");
					String product_desc = inner_rst.getString("Product_Desc");
					double product_price = inner_rst.getDouble("Product_Price");
					// Create new product object
					product = new Product(product_id, product_name, product_desc, product_price);
				}
				// Add CartProduct to list
				cartProductList.add(new CartProduct(cart, product));
			}
			// Return CartProduct list
			return cartProductList;

		} finally {
			// Close JDBC connection
			close(con, pstmt, rst);
		}
	}

	public CartProduct getCartProductItem(String username, int productId) throws Exception {

		Cart cart = null;
		Product product = null;
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rst = null;
		String query;
		try {
			// Connect to DB
			con = dataSource.getConnection();

			// Create SQL statement to get Cart
			query = "SELECT * FROM shopping.cart WHERE username=? AND Product_Id=?";

			// Create prepared statement
			pstmt = con.prepareStatement(query);
			pstmt.setString(1, username);
			pstmt.setInt(2, productId);

			// Execute query
			rst = pstmt.executeQuery();

			// Return cart
			if (rst.next()) {
				int quantity = rst.getInt("Quantity");
				cart = new Cart(username, productId, quantity);
			}

			// Create SQL statement to get Product
			query = "SELECT * FROM shopping.product WHERE Product_Id=?";

			// Create prepared statement
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, productId);

			// Execute query
			rst = pstmt.executeQuery();

			// Return product
			if (rst.next()) {
				String product_name = rst.getString("Product_Name");
				String product_desc = rst.getString("Product_Desc");
				double product_price = rst.getDouble("Product_Price");
				// Create new product object
				product = new Product(productId, product_name, product_desc, product_price);
			}
			// Return CartProduct
			return new CartProduct(cart, product);

		} finally {
			// Close JDBC
			close(con, pstmt, rst);
		}
	}
	
	public void updateShoppingCartItem(Cart cartItem) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rst = null;
		String query;
		try {
			// Connect to DB
			con = dataSource.getConnection();
			// Create query and execute query
			query = "UPDATE shopping.cart SET Quantity = ? WHERE Username = ? AND Product_Id = ?";
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, cartItem.getQuantity());
			pstmt.setString(2, cartItem.getUsername());
			pstmt.setInt(3, cartItem.getProductId());
			pstmt.executeQuery();

		} finally {
			// Close JDBC connection
			close(con, pstmt, rst);
		}
	}

	public void deleteShoppingCartItem(String username, int productId) throws Exception {
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rst = null;
		String query;
		try {
			// Connect to DB
			con = dataSource.getConnection();
			// Create query and execute query
			query = "DELETE FROM shopping.cart WHERE Username = ? AND Product_Id = ?";
			pstmt = con.prepareStatement(query);
			pstmt.setString(1, username);
			pstmt.setInt(2, productId);
			pstmt.executeQuery();

		} finally {
			// Close JDBC connection
			close(con, pstmt, rst);
		}
	}

	public void addCartDetails(String username, int productId, int qty) throws Exception {
		Connection con = null;
		PreparedStatement pstmt=null;
		
		try {
			//create a connection
			con = dataSource.getConnection();
			//create sql query 
			String qry = "insert into shopping.cart" + "(username, product_id, quantity) "+ "values(?,?,?)";
			pstmt = con.prepareStatement(qry);
			//set the parameters for the students
			pstmt.setString(1, username);
			pstmt.setInt(2, productId);
			pstmt.setInt(3, qty);
			//execute the sql query
			pstmt.execute();
			}
			finally
			{
			close(con,pstmt,null);
			}
		}
    
    	private void close(Connection con, Statement stmt, ResultSet rst) {
		try {
			if (rst != null) {
				rst.close();
			}
			if (stmt != null) {
				stmt.close();
			}
			if (con != null) {
				con.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}