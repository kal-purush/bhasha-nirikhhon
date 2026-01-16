package com.kupybaton.web.servlets;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


import com.kupybaton.web.jdbc.update.ListUpdater;

public class ChangeListName extends HttpServlet {
	 @Override
	 public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
	    	
	    	String productlistIdString = request.getParameter("productlistId");
			String listName = request.getParameter("listName");
			
			Integer productlistId;

			if (productlistIdString != null & listName.length()>0) {
				
				try {
					productlistId = Integer.valueOf(productlistIdString);
									
					if (ListUpdater.getListUpdater().updateList(listName, productlistId)) {
						response.sendRedirect(request.getContextPath() + "/purchases.html?productlistId=" + productlistId);

					}
					
				} catch (NumberFormatException nfe) {
					nfe.printStackTrace();
					response.sendRedirect(request.getContextPath() + "/purchases.html?purchaseEditError=true&productlistId=" + productlistIdString);
					return;
				}
			} else {
				response.sendRedirect(request.getContextPath() + "/purchases.html?purchaseEditError=true&productlistId=" + productlistIdString);
			}
		}

}