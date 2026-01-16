package com.shoppingcart.controller;

import java.io.IOException;
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


/**
 * Servlet implementation class ShoppingCartServlet
 */
@WebServlet("/ShoppingCartServlet")
public class CartControllerServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	
	private ShoppingCartUtil shoppingCartUtil;
	private String username = "";

	private int productId;
	private int qty = 0;
	
	List<Cart> completeShoppingCart;
	List<Cart> userShoppingCart;
	Cart cart = null;
	 
	@Resource(name="jdbc/EMP")
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
		response.getWriter().append("Served at: ").append(request.getContextPath());
		
	try {
			String theCommand=request.getParameter("command");
			//if the command is missing then display listing
			if(theCommand==null) {
			theCommand = "LIST";
			}
			
			//routing to appropriate method
			switch(theCommand)
			{
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

		username = request.getParameter("user_name");
		productId = Integer.parseInt(request.getParameter("productId"));
		qty = Integer.parseInt(request.getParameter("quantity"));

		completeShoppingCart.add(new Cart(username, productId, qty));
		// add cart user and qty
		shoppingCartUtil.addCartDetails(username, productId, qty);

		// Send back to the main page student list

		listCartItems(request, response);
	}
	
	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}