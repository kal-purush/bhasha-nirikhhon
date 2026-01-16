package it.begear.corso.servlet;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import it.begear.corso.dao.DAOutente;
import it.begear.corso.dao.DAOutenteImpl;
import it.begear.corso.entity.Utente;

@WebServlet("/LogoutServlet")
public class LogoutServlet extends HttpServlet {
	

	private static final long serialVersionUID = 1L;

	public LogoutServlet() {}

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		HttpSession session = request.getSession(false);   // get la sessione esistente
		if(session != null) {
			session.removeAttribute("loggedIn");    // disconnette l'utente
			session.removeAttribute("loggedIn_ID");    
			session.removeAttribute("loggedIn_NOME");
			session.removeAttribute("loggedIn_COGNOME");
			session.removeAttribute("loggedIn_EMAIL");
			response.sendRedirect("LoginRegister.jsp");
		}
	}
}