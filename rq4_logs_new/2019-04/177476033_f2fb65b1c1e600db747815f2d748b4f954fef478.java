package servlet;


import java.io.FileNotFoundException;
import java.io.FileWriter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

/**
 * Servlet implementation class myFirstServlet
 */
@WebServlet("/SignupServlet")
public class SignupServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @throws FileNotFoundException 
     * @see HttpServlet#HttpServlet()
     */
    public SignupServlet() throws FileNotFoundException {
        super();
        
    }
	protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String next = "";
		String username = request.getParameter("username");
		String pw = request.getParameter("pw");
		String imgurl = request.getParameter("imageurl");
		username = username.trim();
		pw = pw.trim();
		if (username.equals("") || pw.equals("") || imgurl.equals("")) {
		if (username.equals(""))
		{
			next = "/signup.jsp";
			request.setAttribute("uerror", "No Username Entered");
			//System.out.println("U empty");
		}
		if (pw.equals(""))
		{
			next = "/signup.jsp";
			request.setAttribute("perror", "No Password Entered");
			//System.out.println("P empty");
		}
		if (imgurl.equals(""))
		{
			next = "/signup.jsp";
			request.setAttribute("ierror", "No ImageURL Entered");
			//System.out.println("P empty");
		}
		}
		else
		{
			int check =0; // do database verification right now, if username is already registered, don't do it
			if (check == 1)
			{
				next = "/signup.jsp";
				request.setAttribute("uerror", "Username Taken");
			}
			else //verification passed, add to database, go back to sign up
			{
				next = "/signup.jsp";
			}
		}
		RequestDispatcher dispatch = getServletContext().getRequestDispatcher(next);
		dispatch.forward(request, response);
	}
}