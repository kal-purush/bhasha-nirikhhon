/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Servlets;

import java.io.IOException;
import java.io.PrintWriter;
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
public class NewInclassCourse extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        response.setContentType("text/html");
        PrintWriter out = response.getWriter();

        String i = request.getParameter("id");
        String co = request.getParameter("code");
        String cd = request.getParameter("credit");
        String p = request.getParameter("professor");
        String l = request.getParameter("location");
        

        try {
            Class.forName("org.apache.derby.jdbc.ClientDriver");
            java.sql.Connection con = DriverManager.getConnection("jdbc:derby://localhost:1527/SE554DB",
                    "APP", "APP");

            PreparedStatement ps = con.prepareStatement(
                    "insert into INCLASSCOURSE (ID, CODE, CREDITHOURS, INSTRUCTOR, LOCATION) values(?,?,?,?,?)");

            ps.setString(1, i);
            ps.setString(2, co);
            ps.setString(3, cd);
            ps.setString(4, p);
            ps.setString(5, l);
            
            int ii = ps.executeUpdate();
            if (ii > 0) {
                out.print("You have successfully created : " + co);
                
                out.println("<a href='admin_dashboard.jsp'>Go to Dashboard</a>");
            }

        } catch (Exception e2) {
            System.out.println(e2);
        }

        out.close();

    }
}