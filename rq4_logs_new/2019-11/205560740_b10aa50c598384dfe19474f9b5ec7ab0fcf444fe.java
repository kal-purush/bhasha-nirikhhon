package com.controler;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.dao.LoginDao;
import com.user.Login;

/**
 * Servlet implementation class LoginServlet
 */
@WebServlet("/LoginServlet")
public class LoginServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
    
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		//登陆请求
		request.setCharacterEncoding("utf-8");
		String uname=request.getParameter("uname");
		String pwd = request.getParameter("pwd");
		Login login=new Login(uname,pwd);
		System.out.println(login);
		
		int result=LoginDao.login(login);
		System.out.println(result);
		if (result>0) {
			request.getRequestDispatcher("uhelp.jsp").forward(request,response);
//			response.sendRedirect("uhelp.jsp");
			System.out.println("登陆成功");
		} else {
			response.sendRedirect("login.jsp");
//			JOptionPane jOptionPane = JOptionPane;
//			jOptionPane.showMessageDialog(null, "用户名或密码错误");
//			System.exit(0);
			System.out.println("登陆失败");
		}
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}