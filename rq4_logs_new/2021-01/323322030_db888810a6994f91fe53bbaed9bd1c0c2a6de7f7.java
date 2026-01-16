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

import it.begear.corso.dao.DAOutenteImpl;
import it.begear.corso.entity.Utente;



@WebServlet("/NewsletterServlet")
public class NewsletterServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	
	
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {		
		ApplicationContext context = new ClassPathXmlApplicationContext("Beans.xml");
		DAOutenteImpl daoUtente = context.getBean(DAOutenteImpl.class);
		HttpSession session = request.getSession(false);           // mettiamo false per non fare creare una nuova sessione,
		
		String subscribe = (String) request.getParameter("subscribe-email");
		String destP = "subscribe.jsp";
		
		if (subscribe.equals("cancellato")) { 
			 Utente utente = (Utente) session.getAttribute("loggedIn");
			 if(utente.isSubscribed()) {
				 utente.setSubscribed(false);                        // cancella l'iscrizione
	 		     daoUtente.update(utente, utente.getId());         // aggiorna nel DB
	 		     session.setAttribute("loggedIn", utente);         // aggiorna nella sessione
	 		     
	 		     response.sendRedirect("account.jsp?cancellato=OK");
			 }
			 else {
				 response.sendRedirect("account.jsp?cancellato=NO");    // utente non e' iscritto
			 }
		}
		else {
			Utente utente = (Utente) session.getAttribute("loggedIn"); 
 		    utente.setSubscribed(true);                        // utente iscritto nella newsletter
 		    daoUtente.update(utente, utente.getId());         // aggiorna nel DB
 		    session.setAttribute("loggedIn", utente); 
 		    
 		   response.sendRedirect("subscribe.jsp?iscriviti=OK");
		}    	
	}
	
	
	
}