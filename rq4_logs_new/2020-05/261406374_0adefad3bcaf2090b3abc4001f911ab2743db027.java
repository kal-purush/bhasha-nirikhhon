package com.kh.admin.controller;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.kh.admin.model.service.AdminService;

/**
 * Servlet implementation class AdminGrantLectorEnd
 */
@WebServlet("/admin/adminGrantLectorEnd")
public class AdminGrantLectorEndServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public AdminGrantLectorEndServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String lectorNo=request.getParameter("lecNo");
		String optradio=request.getParameter("optradio");
		int result=0;
		if(optradio.equals("Y")) {
			result=new AdminService().updateGrantLector(lectorNo);
			request.setAttribute("msg", lectorNo+"번 강좌가 승인되었습니다");
			request.setAttribute("loc", "/admin/adminGrantLector");
		}else if(optradio.equals("N")) {
			result=new AdminService().updateRejectLector(lectorNo);			
			request.setAttribute("msg", lectorNo+"번 강좌가 거절되었습니다");
			request.setAttribute("loc", "/admin/adminGrantLector");
		}
		request.getRequestDispatcher("/views/common/msg.jsp").forward(request, response);
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}