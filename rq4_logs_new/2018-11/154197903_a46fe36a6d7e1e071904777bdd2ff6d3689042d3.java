package controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


@WebServlet(name = "LoginDemo", urlPatterns = {"/LoginDemo", "/logindemo", "/acesso"})
public class LoginDemo extends HttpServlet {


    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        
    }

    
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/html;charset=UTF-8");
        
                String login=request.getParameter("login");
		String senha=request.getParameter("senha");
                
        try (PrintWriter out = response.getWriter()) {
            Class.forName("com.mysql.jdbc.Driver");
		 
		Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/rscsis", "root", "root"); 
 
		PreparedStatement ps = conn.prepareStatement("select * from usuario where login=? and senha=?");
		ps.setString(1, login);
		ps.setString(2, senha);
 
		ResultSet rs = ps.executeQuery();
 
		while (rs.next()) {
			response.sendRedirect("Su.html");
			return;
		}
		response.sendRedirect("Fa.html");
		return;
		}
                catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>

}