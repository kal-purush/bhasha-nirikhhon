
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

public class DelandUpdate extends HttpServlet {
	// use void service when its starts and use doPost for hiding credentials

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// TODO Auto-generated method stub
		response.setContentType("text/html");
		Connection con = null;
		try {
			PrintWriter pw = response.getWriter();
			String s1 = request.getParameter("pass1");
			String s2 = request.getParameter("usr");
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection("jdbc:mysql://localhost:3306/project", "root", "Rupesh#1");
			PreparedStatement p1;
			p1 = con.prepareStatement("update login set PWD=? where USR=?");
			p1.setString(1, s1);
			p1.setString(2, s2);
			int i = p1.executeUpdate();
			p1.close();
			con.close();
			if (i > 0) {
				pw.println("<script type=\"text/javascript\">");
				pw.println("alert('Password Reset Successfully');");
				pw.println("location='Login.jsp';");
				pw.println("</script>");
			} else {
				pw.println("<h1> Admin of given Username doesn't Exist..</h1>");
				pw.print("<br><a href=\"Login.jsp\"><button type=\"submit\">Return to Admin Login</button></a>");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}