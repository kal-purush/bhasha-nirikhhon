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



public class Listing_Adding extends HttpServlet {
	
	public void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException, IOException{
		WebServiceCategorieProxy categorieProxy = new WebServiceCategorieProxy();
		
		String categorie_id_string = (String) request.getParameter("categorie_id");
		
		//Si on arrive sur cette page après une demande de suppresion
		if(categorie_id_string != null)
			categorieProxy.delCategorie((int) Long.valueOf(categorie_id_string).longValue());
			
			
		//Récupère toutes les catégories pour les afficher
        Categorie[] array_cat = categorieProxy.getCategories();
        
        request.setAttribute("liste_categorie", array_cat);
        this.getServletContext().getRequestDispatcher( "/categorie/liste.jsp" ).forward( request, response );
    }
	
	public void doPost( HttpServletRequest request, HttpServletResponse response ) throws ServletException, IOException{
		//Récupère les champs pouvant avoir été modifiés
		String updated_nom = (String) request.getParameter("name");
		WebServiceCategorieProxy categorieProxy = new WebServiceCategorieProxy();
        
		//Requete le webservice pour ajouter une catégorie
		categorieProxy.newCategorie(updated_nom);
      
		//Récupère toutes les catégories pour les afficher
		Categorie[] array_cat = categorieProxy.getCategories();
        
		//TODO vérifier dans la JSP que le champ n'est pas nul
        request.setAttribute("liste_categorie", array_cat);
		this.getServletContext().getRequestDispatcher( "/categorie/liste.jsp" ).forward( request, response );
    }
}