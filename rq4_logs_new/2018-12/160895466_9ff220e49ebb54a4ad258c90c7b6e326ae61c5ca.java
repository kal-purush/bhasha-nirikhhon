

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class Addauthor
 */
@WebServlet("/Addauthor")
public class Addauthor extends HttpServlet {
		protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		response.getWriter().append("Served at: ").append(request.getContextPath());
		response.getWriter().append("\nAdded successfully.");
		
		
		String aid=request.getParameter("aid");
		String aname=request.getParameter("aname");
		String alink=request.getParameter("alink");
		// JDBC driver name and database URL
		final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
		final String DB_URL = "jdbc:mysql://localhost/Prototype1";

		//  Database credentials
		final String USER = "root";
		final String PASS = "root";
		   
		  
		Connection conn = null;
		Statement stmt = null;
		
		try{
			//STEP 2: Register JDBC driver
		    Class.forName("com.mysql.jdbc.Driver");

		    //STEP 3: Open a connection
		    System.out.println("Connecting to database...");
		    conn = DriverManager.getConnection(DB_URL,USER,PASS);

		    //STEP 4: Execute a query
		    System.out.println("Creating statement...");
		    stmt = conn.createStatement();
		      
		    stmt.executeUpdate("INSERT INTO author(aid,aname,alink)"+"values('"+aid+"','"+aname+"','"+alink+"')");

		
		    System.out.println("Success");
		    stmt.close();
		    conn.close();
		}catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
		}catch(Exception e){
			//Handle errors for Class.forName
		    e.printStackTrace();
		}finally{
			//finally block used to close resources
		    try{
		             if(stmt!=null)
		        	 stmt.close();
		    }catch(SQLException se2){
		    	}// nothing we can do
		        try{
		        	if(conn!=null)
		            conn.close();
		        }catch(SQLException se){
		         se.printStackTrace();
		      }//end finally try
		   }//end try
		   System.out.println("\nGoodbye!");
		   return;
		
	

	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
		response.sendRedirect("success.jsp");

	}

}