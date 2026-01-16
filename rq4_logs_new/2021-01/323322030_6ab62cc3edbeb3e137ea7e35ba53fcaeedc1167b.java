package it.begear.corso.servlet;

import java.io.IOException;
import java.util.List;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.jasper.tagplugins.jstl.core.Out;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import it.begear.corso.dao.DAOscarpaImpl;
import it.begear.corso.entity.Genere;
import it.begear.corso.entity.Scarpa;



@WebServlet("/GenereServlet")
public class GenereServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		ApplicationContext context = new ClassPathXmlApplicationContext("Beans.xml");
		DAOscarpaImpl daoscarpa = context.getBean(DAOscarpaImpl.class);
		
		String g = (String) request.getParameter("uomo");
		Genere genereScarpa = Genere.valueOf(g);
		String parameters = "begear";
		
//		if (genereScarpa != null) {
//			List<Scarpa> scarpaList = daoscarpa.findByGenere(genereScarpa);
//			
//			int index = 1, counter = 1;
//			for(Scarpa scarpa : scarpaList) {
//				if(counter != 3) {
//					parameters += "<div class='product_box'>"
//							   +      "<h3>" + scarpa.getCodice() + "</h3>"
//							   +      "<a href='#'><img src='IMAGES/scarpe/" + scarpa.getCodice() + ".jpg' alt='Shoes " + index + "'/></a>"
//							   +      "<p>" + scarpa.getDescrizione() + "</p>"
//							   +    "<p class='product_price'>" + scarpa.getCosto() + "€ </p>"
//							   +      "<a href='AddItemCartServlet?codice=" + scarpa.getCodice() + "&amp;quantita=1' class='addtocart'></a>"
//							   +      "<a href='DetailItemServlet?codice=" + scarpa.getCodice() + "' class='detail'></a>"
//							   +  "</div> \n";
//					counter = 1;
//				} else {
//					parameters += "<div class='product_box no_margin_right'>"
//							   +      "<h3>" + scarpa.getCodice() + "</h3>"
//							   +      "<a href='#'><img src='IMAGES/scarpe/" + scarpa.getCodice() + ".jpg' alt='Shoes " + index + "'/></a>"
//							   +      "<p>" + scarpa.getDescrizione() + "</p>"
//							   +    "<p class='product_price'>" + scarpa.getCosto() + "€ </p>"
//							   +      "<a href='AddItemCartServlet?codice=" + scarpa.getCodice() + "&amp;quantita=1' class='addtocart'></a>"
//							   +      "<a href='DetailItemServlet?codice=" + scarpa.getCodice() + "' class='detail'></a>"
//							   +  "</div> \n";
//					
//					parameters += "<div class='cleaner'></div> \n";
//					counter++;
//				}
//				index++;
//			}
			request.setAttribute("genere", parameters);
			RequestDispatcher dispatcher = request.getRequestDispatcher("uomoProducts.jsp");
			dispatcher.forward(request, response);
			   
//		} else {
//			response.sendRedirect("404.html?foundProduct=NO");  // not found page and product (scarpa)
//		}     	
	}

	
	
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}
	
	
	
}