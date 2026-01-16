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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author Advait
 */
public class ViewProfessor extends HttpServlet {

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/html");
        PrintWriter out = response.getWriter();
        //String name = request.getParameter("uname");
        try {
            Class.forName("org.apache.derby.jdbc.ClientDriver");
            java.sql.Connection con = DriverManager.getConnection("jdbc:derby://localhost:1527/SE554DB",
                    "APP", "APP");
            //PreparedStatement ps = con.prepareStatement("select * from userlogin where name=?");
            PreparedStatement ps = con.prepareStatement("select * from Instructor");
            //ps.setString(1, name);
            //out.print("<table width=25% border=1>");
            //out.print("<center><h1>Result:</h1></center>");
            ResultSet rs = ps.executeQuery();
            /* Printing column names */
            ResultSetMetaData rsmd = rs.getMetaData();
            while (rs.next()) {
                out.print("<table width=25% border=1 align='center'>");
            out.print("<center><h1>Result:</h1></center>");
                out.print("<tr>");
                out.print("<td>" + rsmd.getColumnName(1) + "</td>");
                out.print("<td>" + rs.getString(1) + "</td></tr>");
                out.print("<tr><td>" + rsmd.getColumnName(2) + "</td>");
                out.print("<td>" + rs.getString(2) + "</td></tr>");
                out.print("<tr><td>" + rsmd.getColumnName(3) + "</td>");
                out.print("<td>" + rs.getString(3) + "</td></tr>");
                out.print("<tr><td>" + rsmd.getColumnName(4) + "</td>");
                out.print("<td>" + rs.getString(4) + "</td></tr>");
                out.print("<tr><td>" + rsmd.getColumnName(5) + "</td>");
                out.print("<td>" + rs.getString(5) + "</td></tr>");
                out.print("<tr><td>" + rsmd.getColumnName(6) + "</td>");
                out.print("<td>" + rs.getString(6) + "</td></tr>");
                out.print("</table>");
            }
            

        } catch (Exception e2) {
            e2.printStackTrace();
        } finally {
            out.close();
        }
    }
}