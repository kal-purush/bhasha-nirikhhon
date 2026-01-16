package com.notable.controllers;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import com.javatpoint.beans.Emp;
import com.notable.business.User;
import com.notable.data.UserIO;
import com.notable.data.UserMapper;

@Controller
public class UserController {

	@Autowired
	JdbcTemplate jdbc;
	
    /* It provides list of employees in model object */  
    @RequestMapping("/account")  
    public String viewemp(Model m){  
        List<Emp> list= dao.getEmployees();  
        m.addAttribute("list",list);
        return "viewemp";  
    }  
	
	
	
	
	
	
	
	
	
	
	
	/*
	 * @Override public void doGet(HttpServletRequest request, HttpServletResponse
	 * response) throws IOException, ServletException {
	 * 
	 * String url = "register.jsp";
	 * 
	 * getServletContext().getRequestDispatcher(url).forward(request, response); }
	 * 
	 * @Override public void doPost(HttpServletRequest request, HttpServletResponse
	 * response) throws IOException, ServletException { String action =
	 * request.getParameter("action");
	 * 
	 * String url = ""; if (action.equals("register")) { url = registerUser(request,
	 * response); }
	 * 
	 * getServletContext().getRequestDispatcher(url).forward(request, response); }
	 * 
	 * private String registerUser(HttpServletRequest request, HttpServletResponse
	 * response) {
	 * 
	 * // get user data String email = request.getParameter("email"); String
	 * firstName = request.getParameter("firstName"); String lastName =
	 * request.getParameter("lastName"); String address1 =
	 * request.getParameter("address1"); String address2 =
	 * request.getParameter("address2"); String city = request.getParameter("city");
	 * String state = request.getParameter("state"); String zip =
	 * request.getParameter("zip"); String country =
	 * request.getParameter("country"); String password =
	 * request.getParameter("password");
	 * 
	 * // Store in user bean User user = new User(); user.setEmail(email);
	 * user.setFirstName(firstName); user.setLastName(lastName);
	 * user.setAddress1(address1); user.setAddress2(address2); user.setCity(city);
	 * user.setState(state); user.setZip(zip); user.setCountry(country);
	 * user.setPassword(password);
	 * 
	 * // write to a file ServletContext sc = getServletContext(); String path =
	 * sc.getRealPath("/WEB-INF/UserList.txt"); UserIO.add(user, path);
	 * 
	 * // Session storage HttpSession session = request.getSession();
	 * session.setAttribute("user", user);
	 * 
	 * // add cookie Cookie c = new Cookie("myCookie", email); c.setMaxAge(60 * 60 *
	 * 24 * 365 * 2); c.setPath("/"); response.addCookie(c);
	 * 
	 * // return url String url = "/login.jsp"; return url;
	 * 
	 * }
	 */
}