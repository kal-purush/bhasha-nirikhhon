import java.sql.*;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class AccountCode
 */
@WebServlet("/AccountCode")
public class AccountCode extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public AccountCode() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		response.setContentType("text/html");
		PrintWriter out=response.getWriter();
		
		int account_no=Integer.parseInt(request.getParameter("ac_no"));
		String name=request.getParameter("ac_name");
		String password=request.getParameter("psw");
		double amount=Double.parseDouble(request.getParameter("amount"));
		String address=request.getParameter("adrs");
		String mobileno=request.getParameter("mobileno");
		try {
		
			Class.forName("com.mysql.cj.jdbc.Driver");
			Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/bankapp", "root", "root");
			PreparedStatement  ps=con.prepareStatement("insert into account values(?,?,?,?,?,?)");
			ps.setInt(1, account_no);
			ps.setString(2, name);
			ps.setString(3, password);
			ps.setDouble(4, amount);
			ps.setString(5, address);
			ps.setString(6, mobileno);
			
			int i =ps.executeUpdate();			
			if(i>0) {
				response.sendRedirect("success.html");
				
			}
			else {
				out.print("Registration is Failed");
				
			}
			con.close();
			
			
		}
		catch(Exception ex) {
			out.print(ex);
		}
		
	}

}