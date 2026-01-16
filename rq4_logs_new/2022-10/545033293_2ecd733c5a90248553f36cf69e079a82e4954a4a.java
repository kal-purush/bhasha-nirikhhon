package Servlet;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;


@WebServlet(name = "Second1", urlPatterns = {"/second1"})
public class MyServlet3 extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        String name = req.getParameter("name");

        Calendar cal = new GregorianCalendar();
        Date date = new Date(name);
        cal.setTime(date);

        resp.setContentType("text/html");
        PrintWriter writer = resp.getWriter();

        writer.println("<h1>Num = " + (cal.get(Calendar.DAY_OF_YEAR)) + "!</h1>");
    }
}