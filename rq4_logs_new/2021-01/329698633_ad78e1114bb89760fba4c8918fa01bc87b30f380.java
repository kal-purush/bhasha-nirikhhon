package com.hari.controller;

import java.io.IOException;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.hari.dto.Student;
import com.hari.factory.StudentServiceFactory;
import com.hari.service.StudentService;

@WebServlet("/DeleteServlet")
public class DeleteServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String sid=request.getParameter("sno");
		StudentService ss=StudentServiceFactory.getStudentService();
		RequestDispatcher req=null;
		String status=ss.deleteStudent(sid);
		if(status=="success")
		{
			req = request.getRequestDispatcher("success.jsp");
			req.forward(request, response);
		}
		if (status.equalsIgnoreCase("fail")) {
			req = request.getRequestDispatcher("fail.jsp");
			req.forward(request, response);
		}
		
	}

}