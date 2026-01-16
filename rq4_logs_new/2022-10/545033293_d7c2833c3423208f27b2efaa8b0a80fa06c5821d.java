package Servlet;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Task1 {

    @WebServlet(name = "Second", urlPatterns = {"/second"})
    public class MyServlet2 extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

            String name = req.getParameter("name");

            Date format1 = null;
            try {
                format1 = new SimpleDateFormat("dd/MM/yyyy").parse(name);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            DateFormat format2 = new SimpleDateFormat("EEEE");
            String finalDay = format2.format(format1);

            resp.setContentType("text/html");
            PrintWriter writer = resp.getWriter();

            writer.println("<h1>Num of week= " + finalDay + "!</h1>");


        }
    }

}