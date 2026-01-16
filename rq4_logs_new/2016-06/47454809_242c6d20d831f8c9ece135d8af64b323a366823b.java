package servlet;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.annotation.Resource;
import javax.imageio.ImageIO;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import models.FilmCategorie;

/**
 * Servlet implementation class CreateFilmCategory
 */
@WebServlet("/MovieCategories")
public class MovieCategories extends HttpServlet {
	private static final long serialVersionUID = 1L;
	@Resource private DataSource myDataSource;
    /**
     * @see HttpServlet#HttpServlet()
     */
    public MovieCategories() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		 try
		 {
			 Connection myConnection = myDataSource.getConnection("APP","APP");			
			 System.out.println("i am in do GET Categorie, Got Connection!");
		
		
			 String StatementStr11 = "select * from FILM_CATEGORIE order by LIBELLE_CATEGORIE";
			 List<FilmCategorie> categorieList = new ArrayList<FilmCategorie>();
			 PreparedStatement myStatement11 = myConnection.prepareStatement(StatementStr11);
			 ResultSet ResultSet11 = myStatement11.executeQuery();			
			
			 while(ResultSet11.next()) 
			 {
				int idCategorie = ResultSet11.getInt("ID_FILM_CATEGORIE");
				String libelle = ResultSet11.getString("LIBELLE_CATEGORIE");
				FilmCategorie fc = new FilmCategorie(idCategorie , libelle);
				categorieList.add(fc);
			 }
		
			 request.setAttribute("listCategorie", categorieList);		
			 System.out.println(">>>> Query result is true");
			 //response.sendRedirect("./CreateFilmData");
			 //request.setAttribute("CreateMessage", "* Movie Created successfully !");
			 request.getRequestDispatcher("/MovieCategoriesPage.jsp").forward(request, response);
		 } 		
		 catch (SQLException e) 
		 {
			System.out.println(">>>> function Exception");
			request.setAttribute("CreateMessage", "* Categorie Page Load Failed!");
            
			request.getRequestDispatcher("/MovieCategoriesPage").forward(request, response);
			System.out.println(e.getMessage());
			e.printStackTrace();
		 }
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		try 
		{
			//getting Request data
			String CategorieLibelle = request.getParameter("txtCategorieLibelle");
			
	        	      
	        //Setting up database connection
	        Connection myConnection = myDataSource.getConnection("APP","APP");			
	        System.out.println("i am in do POST Categorie, Got Connection!");
			//PreparedStatement myStatement = myConnection.prepareStatement("SELECT * from USERS WHERE USERNAME = '" + user + "' AND PASSWORD_USER = '" + pass + "' ");
	        	       
	        
	        String StatementStr1 = "select * from FILM_CATEGORIE where LIBELLE_CATEGORIE = '" + CategorieLibelle + "'";
			
			PreparedStatement myStatement1 = myConnection.prepareStatement(StatementStr1);
			ResultSet ResultSet1 = myStatement1.executeQuery();			
			
			
			if(ResultSet1.next()) 
			{
				System.out.println(">>>> function Exception");
				request.setAttribute("CreateMessage", "* Movie Categorie already exists!");
	            doGet(request, response);					
			}
			else
			{				
		        PreparedStatement myStatement = myConnection.prepareStatement("INSERT INTO FILM_CATEGORIE (LIBELLE_CATEGORIE) VALUES ('" + CategorieLibelle + "')");	      	       
				
		        //Authentication
				int myResultSet = myStatement.executeUpdate();  //executeQuery();
				 System.out.println("i am in do POST Register, Executed Update Statement!");
				//ResultSet myResultSet = myStatement.executeQuery("SELECT * from Users WHERE Username = \"" + user+ "\" AND Password = \"" + pass + "\" ");
				
				if (myResultSet == 1) 
				{			
					 String StatementStr11 = "select * from FILM_CATEGORIE order by LIBELLE_CATEGORIE";
					List<FilmCategorie> categorieList = new ArrayList<FilmCategorie>();
					PreparedStatement myStatement11 = myConnection.prepareStatement(StatementStr11);
					ResultSet ResultSet11 = myStatement11.executeQuery();			
					
					while(ResultSet11.next()) 
					{
						int idCategorie = ResultSet11.getInt("ID_FILM_CATEGORIE");
						String libelle = ResultSet11.getString("LIBELLE_CATEGORIE");
						FilmCategorie fc = new FilmCategorie(idCategorie , libelle);
						categorieList.add(fc);
					}
				
					request.setAttribute("listCategorie", categorieList);		
					System.out.println(">>>> Query result is true");
					//response.sendRedirect("./CreateFilmData");
					request.setAttribute("CreateMessage", "* Movie Created successfully !");
					//request.getRequestDispatcher("./CreateFilmData").forward(request, response);
					doGet(request, response);
	            } 
	            else 
	            {            	
	            	System.out.println(">>>> Query result is false");
	            	request.setAttribute("CreateMessage", "* Movie Creation Failed!");
	                //request.getRequestDispatcher("/MoviesCreate.jsp").forward(request, response);
	                doGet(request, response);
	            }
			}
		} 		
		catch (SQLException e) 
		{
			System.out.println(">>>> function Exception");
			request.setAttribute("CreateMessage", "* Categorie Creation Failed!");
            
            doGet(request, response);
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

}