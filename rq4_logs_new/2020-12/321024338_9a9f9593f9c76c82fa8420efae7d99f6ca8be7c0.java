package rw.customer.controller;

import java.io.IOException;
import java.util.ArrayList;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import rw.customer.model.service.CostomerService;
import rw.faq.model.vo.Faq;
import rw.notice.model.vo.Notice;

/**
 * Servlet implementation class MainServlet
 */
@WebServlet("/custmoer_center.rw")
public class CustomerServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public CustomerServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	
		ArrayList <Notice> nl = new CostomerService().selectNotice();
		ArrayList <Faq> fl = new CostomerService().selectFAQ();
		
		
		RequestDispatcher view = request.getRequestDispatcher("/views/member/customer_center.jsp");
		
		
		//이미 있는 vo와 혼동하지 않아야함 .. 임의적으로 명칭 지은 것. 
		//기존 List VO 는 네비 + list 
		request.setAttribute("noticeList", nl);
		request.setAttribute("faqList", fl);
	
		
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