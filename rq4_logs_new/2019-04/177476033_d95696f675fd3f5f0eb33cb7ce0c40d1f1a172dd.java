package servlet;

import java.io.FileNotFoundException;

import java.io.IOException;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;



import database_manager.GroceryDataManager;
import database_manager.RecipeDataManager;
import database_manager.RestaurantDataManager;
import database_manager.HistoryDataManager;
import database_manager.UserDataManager;
import info.*;





/**
 * Servlet implementation class myFirstServlet
 */
@WebServlet("/LoginServlet")
public class LoginServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @throws FileNotFoundException
	 * @see HttpServlet#HttpServlet()
	 */
	public LoginServlet() throws FileNotFoundException {
		super();

	}

	protected void service(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String next = "";
		String username = request.getParameter("username");
		String pw = request.getParameter("pw");
		username = username.trim();
		pw = pw.trim();
		String url = "";
		if (username.equals("") || pw.equals("")) {
			if (username.equals("")) {
				next = "/login.jsp";
				request.setAttribute("uerror", "No Username Entered");
			}
			if (pw.equals("")) {
				next = "/login.jsp";
				request.setAttribute("perror", "No Password Entered");
			}
		} else {
			UserDataManager userDB = new UserDataManager(username);
			
			int check = userDB.checkPassword(username,pw);
			System.out.println(check);
			if (check != 1) {
				next = "/login.jsp";
				request.setAttribute("uerror", "Username or Password wrong");
				request.setAttribute("perror", "Username or Password wrong");
			} else {
				next = "/searchPage.jsp"; //successfuly login
				response.setContentType("text/html"); // what's this for?
			}
		}
		RequestDispatcher dispatch = getServletContext().getRequestDispatcher(next);
		dispatch.forward(request, response);
	}
}