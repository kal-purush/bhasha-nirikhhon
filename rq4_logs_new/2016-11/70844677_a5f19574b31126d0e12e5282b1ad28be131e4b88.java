package com.exigenservices.lectures.servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;

/**
 * Created by Дарья on 10.11.2016.
 */
public class DemoServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {

        res.setContentType("text/html");
        Date date = new Date();

        PrintWriter writer = res.getWriter();
        writer.println("<html>");
        writer.println("<head><title>My first page</title></head>");
        writer.println("<body>"+date.toString()+"</body>");
        writer.println("</html>");
        writer.close();
    }
}