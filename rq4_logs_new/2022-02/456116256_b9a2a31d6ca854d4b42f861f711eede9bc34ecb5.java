

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Servlet implementation class FeedbackServlet
 */
@WebServlet("/FeedbackServlet")
public class FeedbackServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public FeedbackServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		response.getWriter().append("Served at: ").append(request.getContextPath());
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		
		PrintWriter out = response.getWriter();
		String n = request.getParameter("question1");
		String p = request.getParameter("question2");
		String e = request.getParameter("question3");
		String c = request.getParameter("question4");
		
		try {
			 Class.forName("com.mysql.jdbc.Driver");
			 Connection con = DriverManager.getConnection(
			 "jdbc:mysql://localhost:3306/feedbacklistlatest", "root", "password");
			 
			 PreparedStatement ps = con.prepareStatement("insert into FEEDBACKLISTLATEST values(?,?,?,?)");
			 ps.setString(1, n);
			 ps.setString(2, p);
			 ps.setString(3, e);
			 ps.setString(4, c);
			 //Step 6: perform the query on the database using the prepared statement
			 int i = ps.executeUpdate();
			 
			 if (i > 0){
				 PrintWriter writer = response.getWriter();
				 writer.println("<h4>" + "Thank You For Submiting your feedback. We will take your Feed Back in consideration to improve on the web app." +
				 "</h4>");
				 writer.close();
				 }
			}
			
		
			catch (Exception exception) {
			 System.out.println(exception);
			 out.close();
			}
		doGet(request, response);
	}

}