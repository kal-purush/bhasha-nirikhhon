package rw.admin.faq.controller;

import java.io.IOException;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import rw.admin.faq.model.service.FAQService;

/**
 * Servlet implementation class NoticeDeleteServlet
 */
@WebServlet("/deleteFAQ.ad")
public class FAQDeleteServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public FAQDeleteServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		

		int faqNo = Integer.parseInt(request.getParameter("faqNo"));
		
		int result = new FAQService().deleteFAQ(faqNo);
		
		
		RequestDispatcher view = request.getRequestDispatcher("/admin/faq/faq_delete_result.jsp");
		
		
		
		if(result>0) {
			
			request.setAttribute("result", true);
			
			
		}else {
			
			request.setAttribute("result", false);
			
		}
		
		
		
		view.forward(request, response);
		
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}