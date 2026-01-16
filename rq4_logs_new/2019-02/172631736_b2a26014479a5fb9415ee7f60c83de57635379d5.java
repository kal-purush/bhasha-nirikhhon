package com.controller.product;

import java.io.IOException;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.dto.ProductCartDTO;
import com.service.ProductCartService;

@WebServlet("/ProductBuyInstantServlet")
public class ProductBuyInstantServlet extends HttpServlet {

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		String userid = "admin";
		
		String pCode = request.getParameter("pCode");
		String pName = request.getParameter("pName");
		String pPrice = request.getParameter("pPrice");
		String pSize = request.getParameter("pSize");
		String pColor = request.getParameter("pColor");
		String pAmount = request.getParameter("pAmount");
		String pImage = request.getParameter("pImage");
		
		ProductCartService service = new ProductCartService();
		// seqnum 가져오기
		int num = service.Seqnum();
		ProductCartDTO dto = new ProductCartDTO(num,userid,pCode,pName,Integer.parseInt(pPrice),pSize,
				Integer.parseInt(pAmount),pImage,pColor);
		request.setAttribute("buyinstant", dto);
		
		RequestDispatcher dis = request.getRequestDispatcher("productBuyInstant.jsp");
		dis.forward(request, response);
		
		
		
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}

}