/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Servlets;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author Advait
 */
public class NewAdmin extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        response.setContentType("text/html");
        PrintWriter out = response.getWriter();

        String id = request.getParameter("id");
        String name = request.getParameter("name");
        String email = request.getParameter("email");
        String username = request.getParameter("username");
        String password = request.getParameter("password");

        try {
            Class.forName("org.apache.derby.jdbc.ClientDriver");
            java.sql.Connection con = DriverManager.getConnection("jdbc:derby://localhost:1527/SE554DB",
                    "APP", "APP");

            PreparedStatement ps = con.prepareStatement(
                    "insert into Admin values(?,?,?,?,?)");

            ps.setString(1, id);
            ps.setString(2, name);
            ps.setString(3, email);
            ps.setString(4, password);
            ps.setString(5, username);
            

            int ii = ps.executeUpdate();
            if (ii > 0) {
                out.print("You are successfully registered...");
                
                out.println("<a href='admin.jsp'>Go to Login Page</a>");
            }

        } catch (Exception e2) {
            System.out.println(e2);
        }

        out.close();

    }

}