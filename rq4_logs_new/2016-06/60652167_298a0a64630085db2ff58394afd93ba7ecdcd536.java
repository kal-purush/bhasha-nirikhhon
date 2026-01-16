
package Admin;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;


/**
 * <br>
 * Servlet de logout administrateur.</b>
 * <p>
 * Gestion des réponses aux appels GET et POST
 * </p>
 * 
 * 
 * @author Gabriel Etienne
 * @version 1.2
 */
@SuppressWarnings("serial")
public class AdminLeave extends HttpServlet {

	/**
	 * Méthode de la servlet permettant la gestion de requête GET
	 * 
	 * Log out administrateur 
	 * 
	 * Supprime attribut session
	 * 
	 */
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		/* Récupération de la session depuis la requête */
		HttpSession session = request.getSession();
		
		//empêcher de faire deux fois le même questionnaire par session
		Boolean is_admin = (Boolean) session.getAttribute("admin");
			
		if( is_admin != null && is_admin == true ){
			
			//Pas encore admin
			session.setAttribute("admin", null);
		
		}
		this.getServletContext().getRequestDispatcher("/admin/logout.jsp").forward(request, response);
	}

	/**
	 * Méthode de la servlet permettant la gestion de requête POST
	 * 
	 * Méthode servant à la redirection vers la méthode doGet en cas de requête POST
	 * sur cette page
	 * 
	 */
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}
}