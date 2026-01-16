package com.metacube.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import dao.DatabaseHelper;
import pojo.Employee;

/**
 * Servlet implementation class EmployeeLogin
 */
@WebServlet("/EmployeeLogin")
public class EmployeeLogin extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public EmployeeLogin() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		String employee_email = request.getParameter("employee_login_email");
		String employee_confirm_password = request.getParameter("employee_login_password");
		String email = null , password=null;
		PrintWriter out = response.getWriter();
		
		 ArrayList<Employee> listOfEmployee=new ArrayList<Employee>();
		 listOfEmployee = DatabaseHelper.getEmployee(employee_email,employee_confirm_password);
		
		 createSession(listOfEmployee,response,request,employee_email,employee_confirm_password);
		  
		  
		
		
		  
	}

	

	private void createSession(ArrayList<Employee> listOfEmployee, HttpServletResponse response, HttpServletRequest request, String employee_email, String employee_confirm_password) throws IOException {
		
		
		
		if(listOfEmployee.size()>0 && listOfEmployee.get(0).getEmployee_emailid().equalsIgnoreCase(employee_email) && listOfEmployee.get(0).getEmployee_password().equals(employee_confirm_password)) {
			HttpSession session = request.getSession(true);
			 session.setAttribute("emailid",employee_email);
			 session.setMaxInactiveInterval(1*30); 
			  response.sendRedirect("login.jsp?message=<p style='color:green'>you have successfully login</p>"
		  );
		  
		  } else { response.sendRedirect("login.jsp?message=<p style='color:red'>Incorrect Email id or password</p>"
		  ); }
		 
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}