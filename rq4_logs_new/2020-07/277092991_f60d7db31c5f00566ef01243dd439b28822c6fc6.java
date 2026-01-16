package controllers;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import dao.UserDao;
import models.Users;

/**
 * Servlet implementation class Login
 */
public class Login extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public Login() {
        super();
        // TODO Auto-generated constructor stub
    }

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		HttpSession httpSession=request.getSession();
		boolean status;
		Users user;
		String userEmail=request.getParameter("userEmail");
		String userPassword=request.getParameter("userPassword");
		status=UserDao.validateUser(userEmail, userPassword);
		if(status==true)
		{
			user=UserDao.getUser(userEmail);
			httpSession.setAttribute("user", user);
			response.sendRedirect("showtodos.jsp");
		}
		else
		{
			httpSession.setAttribute("message", "Email or Password is not correct!");
			response.sendRedirect("login.jsp");
		}
		
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}
}
