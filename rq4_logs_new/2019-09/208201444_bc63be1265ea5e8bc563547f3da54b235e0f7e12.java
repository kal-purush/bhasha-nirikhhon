package com.shoppingcart.controller;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import javax.annotation.Resource;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import com.shoppingcart.dataUtil.ShoppingCartUtil;
import com.shoppingcart.entity.Cart;
import com.shoppingcart.entity.CartProduct;
import com.shoppingcart.entity.Product;
import com.shoppingcart.entity.User;


/**
 * Servlet implementation class ShoppingCartServlet
 */
@WebServlet("/CartControllerServlet")
public class CartControllerServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	
	private ShoppingCartUtil shoppingCartUtil;
	private User user;
	
	List<Cart> completeShoppingCart;
	List<Cart> userShoppingCart;
	Cart cart = null;
	 
	@Resource(name="jdbc/shopping")
	private DataSource dataSource;
	
    @Override
	public void init() throws ServletException {
		// TODO Auto-generated method stub
    	
		try {
			
			completeShoppingCart = new ArrayList<Cart>();
			userShoppingCart= new ArrayList<Cart>();
 	shoppingCartUtil = new ShoppingCartUtil(dataSource);
		//super.init();
		}
		catch (Exception e){
			throw new ServletException(e);
		}
		super.init();
	}

	/**
     * @see HttpServlet#HttpServlet()
     */

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		
		
	try {
			String theCommand=request.getParameter("command");
			//if the command is missing then display listing
			if(theCommand==null) {
			theCommand = "LIST";
			}
			
			//routing to appropriate method
			switch(theCommand)
			{
			case "JACKET" :
				fetchJacketItems(request,response);
				break;
			case "PANTS" :
				fetchPantsItems(request,response);
				break;
			case "SHIRT" :
				fetchShirtItems(request,response);
				break;
			case "SHORT" :
				fetchShortItems(request,response);
				break;
			case "SHOE" :
				fetchShoeItems(request,response);
				break;
			case "LIST":
				listCartItems(request,response);
				break;
			case "ADD":	
				addProductToCart(request,response);
				break;
			case "DELETE":	
				deleteShoppingCartItem(request,response);
				break;
			case "UPDATE":	
				updateShoppingCartItem(request,response);
				break;
			case "SELECTCART":	
				displayShoppingCartItemDetails(request,response);
				break;
			default:
				listCartItems(request,response);
				break;
			}
		} 
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			throw new ServletException(e);
		}
	}
	
	
	private void fetchShoeItems(HttpServletRequest request, HttpServletResponse response) throws Exception {

		List<Product> product = shoppingCartUtil.getShoeItems();
		
		
		request.setAttribute("PRODUCT_LIST", product);
		
		RequestDispatcher dispatcher = request.getRequestDispatcher("/catalogue.jsp");
		dispatcher.forward(request, response);
		
	}

	private void fetchShortItems(HttpServletRequest request, HttpServletResponse response) throws Exception {

		List<Product> product = shoppingCartUtil.getShortItems();
		
		
		request.setAttribute("PRODUCT_LIST", product);
		
		RequestDispatcher dispatcher = request.getRequestDispatcher("/catalogue.jsp");
		dispatcher.forward(request, response);
		
	}

	private void fetchShirtItems(HttpServletRequest request, HttpServletResponse response) throws Exception {
		
		List<Product> product = shoppingCartUtil.getShirtItems();
		
		
		request.setAttribute("PRODUCT_LIST", product);
		
		RequestDispatcher dispatcher = request.getRequestDispatcher("/catalogue.jsp");
		dispatcher.forward(request, response);
	}

	private void fetchPantsItems(HttpServletRequest request, HttpServletResponse response) throws Exception {
		
		List<Product> product = shoppingCartUtil.getPantsItems();
		
		
		request.setAttribute("PRODUCT_LIST", product);
		
		RequestDispatcher dispatcher = request.getRequestDispatcher("/catalogue.jsp");
		dispatcher.forward(request, response);
		
	}

	private void fetchJacketItems(HttpServletRequest request, HttpServletResponse response) throws SQLException, ServletException, IOException {
		
		List<Product> product = shoppingCartUtil.getJacketItems();
	
		request.setAttribute("PRODUCT_LIST", product);
		
		RequestDispatcher dispatcher = request.getRequestDispatcher("/catalogue.jsp");
		dispatcher.forward(request, response);
	}

	private void listCartItems(HttpServletRequest request, HttpServletResponse response) throws Exception {

		// Read username
		String username = request.getParameter("username");

		// Get students from shoppingCartUtil, set to request
		List<CartProduct> cartProducts = shoppingCartUtil.getCartItems(username);
		request.setAttribute("CART_ITEMS", cartProducts);

		// Get RequestDispatcher, forward to JSP
		RequestDispatcher dispatcher = request.getRequestDispatcher("/Cart.jsp");
		dispatcher.forward(request, response);
	}

	private void displayShoppingCartItemDetails(HttpServletRequest request, HttpServletResponse response) throws Exception {
		
		// 1. Read username and productId from Cart
		String username = request.getParameter("username");
		int productId = Integer.parseInt(request.getParameter("productId"));

		// 2. Get shoppingCartItem details from DB
		CartProduct cartProductItem = shoppingCartUtil.getCartProductItem(username, productId);

		// 3. Place shoppingPartItem in request attribute
		request.setAttribute("CART_ITEM", cartProductItem);

		// 4. Send to JSP
		RequestDispatcher dispatcher = request.getRequestDispatcher("/update_shoppingcart_item.jsp");
		dispatcher.forward(request, response);

	}

	private void deleteShoppingCartItem(HttpServletRequest request, HttpServletResponse response) throws Exception {
		// 1. Read username and productId from Cart
		String username = request.getParameter("username");
		int productId = Integer.parseInt(request.getParameter("productId"));

		try {
			// 2. Delete from Cart
			shoppingCartUtil.deleteShoppingCartItem(username, productId);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Redirect to list shopping cart items page
		listCartItems(request, response);
	}

	private void updateShoppingCartItem(HttpServletRequest request, HttpServletResponse response) throws Exception {
		// Get add shopping cart item form parameters
		String username = request.getParameter("username");
		int productId = Integer.parseInt(request.getParameter("productId"));
		int quantity = Integer.parseInt(request.getParameter("quantity"));

		// Create new cart item with the parameters
		Cart cartItem = new Cart(username, productId, quantity);

		// Add cart item to DB
		try {
			shoppingCartUtil.updateShoppingCartItem(cartItem);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		listCartItems(request, response);
	}
	private void addProductToCart(HttpServletRequest request, HttpServletResponse response) throws Exception {
		// TODO Auto-generated method stub'
			//get the students from StudentDbUtil

		String username = request.getParameter("username");
		String product_name = request.getParameter("productName");
		String product_desc = request.getParameter("productDesc");
		double product_price = Double.parseDouble(request.getParameter("productPrice"));
		int quantity = Integer.parseInt(request.getParameter("quantity"));
		double total_quantity_price = product_price * quantity;
		
		shoppingCartUtil.addCartDetails(username, product_name, product_desc, product_price, quantity, total_quantity_price );

		request.setAttribute("username", username);
		
		
		// Send back to the main page student list
		RequestDispatcher dispatcher = request.getRequestDispatcher("/catalogue.jsp");
		dispatcher.forward(request, response);
	}
	
	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}