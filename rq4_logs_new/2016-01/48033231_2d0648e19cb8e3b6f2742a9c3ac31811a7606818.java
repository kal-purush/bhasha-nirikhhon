package servlets;


import java.io.IOException;

import javax.ejb.EJB;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import ejb.EtudiantImpl;
import ejb.ProjetImpl;
import ejb.UtilisateurImpl;


/**
 * Servlet implementation class Accueil
 */
@WebServlet("/RetirerEleve")
public class RetirerEleve extends HttpServlet {
	private static final long serialVersionUID = 1L;      
	
	@EJB
	 private UtilisateurImpl   utilisateurImpl;
	@EJB
	 private EtudiantImpl   etudiantImpl;
	@EJB
	 private ProjetImpl   projetImpl;
    
	/**
     * @see HttpServlet#HttpServlet()
     */
    public RetirerEleve() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		HttpSession session= request.getSession(false);
		if(session!=null){
			String admin = (String) session.getAttribute("Admin");
			if (admin == null ){
				// Quand on arrive sur le site pour la premi�re fois
				session.setAttribute("Admin", "Deco");
			}
			else
			{
				session.setAttribute("Admin", admin);
			}
		}
		
		
        this.getServletContext().getRequestDispatcher("/WEB-INF/RetirerEleve.jsp").forward( request, response );
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		doGet(request, response);

	}

}

	
	
