package servlet;

import java.io.IOException;

import javax.servlet.http.*;
import javax.annotation.Resource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import models.Film;
import models.User;

@WebServlet("/Home")
public class HomeServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
    
	@Resource private DataSource myDataSource;
	
    public HomeServlet() 
    {
        super();
    }  

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException 
	{		
		System.out.println("i am in do GET HomeServlet");
		//response.getWriter().append("Served at: ").append(request.getContextPath());
		doPost(request,response);
		System.out.println("i am out of do GET HomeServlet");				 
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException 
	{							
		try 
		{	//Setting up database connection
	        Connection myConnection = myDataSource.getConnection("APP","APP");			
	        System.out.println("i am in do POST HomeServlet, Got Connection!");
			
			//Liste des nouveaux films
			PreparedStatement statementNewFilms;
			statementNewFilms = myConnection.prepareStatement("SELECT * FROM FILM ORDER BY FILM_CREATION_DATE FETCH FIRST 8 ROWS ONLY");
			ResultSet newMoviesResult = statementNewFilms.executeQuery();
			
			List<Film> listF = new ArrayList<Film>();
			while (newMoviesResult.next()) 
			{
				int idFilm = newMoviesResult.getInt("ID_FILM");
				String nomFilm = newMoviesResult.getString("NOM_FILM");
				String descriptionFilm = newMoviesResult.getString("DESCRIPTION_FILM");
				String dateReleased = newMoviesResult.getString("DATE_RELEASED");
				int notationFilm = Integer.parseInt(newMoviesResult.getString("NOTATION_FILM"));
				String trailer = newMoviesResult.getString("TRAILER_FILM_LINK");
				String filmLink =  newMoviesResult.getString("FILM_LINK");
				String image =  newMoviesResult.getString("FILM_IMAGE");
				String creationDate =  newMoviesResult.getString("FILM_CREATION_DATE");
				String modificationDate =  newMoviesResult.getString("FILM_MODIFICATION_DATE");
				Film currentFilm = new Film(idFilm,nomFilm,descriptionFilm,dateReleased,notationFilm,trailer,filmLink,image,creationDate,modificationDate) ;
				listF.add(currentFilm);
				
				int i = 0;
				i++;
				System.out.println("Film " + i + " : " + nomFilm);
			}   
			
			//Liste des People you may know
			
			//Creer un httpsession pour mettre l'objet user
			HttpSession session;
			session = request.getSession();
			session.setAttribute("listHits", listF);		
						
			System.out.println("redirecting to home");
			request.getRequestDispatcher("/Home.jsp").forward(request, response);
		
		
			myConnection.close();
			System.out.println("i am out of do POST HomeServlet");
		} 		
		catch (SQLException e) 
		{ 
			System.out.println(e.getMessage());
			e.printStackTrace();
		}	
	}
}