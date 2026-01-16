package CategorieGestion;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import beans.*;
import WebService.WebServiceCategorieProxy;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class Editing extends HttpServlet {
	
	public void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException, IOException{
		WebServiceCategorieProxy categorieProxy = new WebServiceCategorieProxy();
		
		String categorie_id_string = (String) request.getParameter("categorie_id");
		
		//Récupère toutes les catégories pour les afficher
        Categorie category_a_modifier = categorieProxy.getCategorie((int) Long.valueOf(categorie_id_string).longValue());
        
        request.setAttribute("categorie", category_a_modifier);
        this.getServletContext().getRequestDispatcher( "/categorie/details.jsp" ).forward( request, response );
    }
	
	public void doPost( HttpServletRequest request, HttpServletResponse response ) throws ServletException, IOException{
		//Récupère le nouveau nom de la catégorie
		String updated_nom = (String) request.getParameter("name");

		//Récupère l'id de la catégorie à modifier
		String categorie_id_string = (String) request.getParameter("id");

		
		WebServiceCategorieProxy categorieProxy = new WebServiceCategorieProxy();
        
		//Requete le webservice pour modifier une catégorie
		categorieProxy.modifyCategorie((int) Long.valueOf(categorie_id_string).longValue(), updated_nom);

	
		//Récupère toutes les catégories pour les afficher
        Categorie category_a_modifier = categorieProxy.getCategorie((int) Long.valueOf(categorie_id_string).longValue());
        
        request.setAttribute("categorie", category_a_modifier);
        this.getServletContext().getRequestDispatcher( "/categorie/details.jsp" ).forward( request, response );
    }
}