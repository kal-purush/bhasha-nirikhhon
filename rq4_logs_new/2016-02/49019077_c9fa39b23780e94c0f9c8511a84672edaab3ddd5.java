package com.kupybaton.web.servlets;

import java.io.IOException;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.kupybaton.web.jdbc.create.PurchaseInserter;


public class AddProductToPurchase extends HttpServlet {
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		
		String listIdString = request.getParameter("productlistId");
		String productIdString = request.getParameter("productId");
		String quantityString = request.getParameter("quantity");
		
		Integer listId;
		Integer productId;
		Double quantity;
	        try {
	            listId = Integer.valueOf(listIdString);
	            productId = Integer.valueOf(productIdString);
	            quantity = Double.valueOf(quantityString);
	        } catch (NumberFormatException nfe) {
	            nfe.printStackTrace();
	            response.sendRedirect(request.getContextPath() + "/lists.html?listEditError=true");
	            return;
	        }
	        
	   if (PurchaseInserter.getPurchaseInserter().insertNewPurchase(listId, productId, quantity)) {
		   	
		   response.sendRedirect(request.getContextPath() + "/purchases.html?listId=" + listId);
	       
	   }
	
	    
	}
}