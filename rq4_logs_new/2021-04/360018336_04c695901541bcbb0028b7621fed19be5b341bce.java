package ru.geekbrains.servlet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;

@WebServlet(name = "FirstServlet", urlPatterns = "/first_servlet")
public class FirstServlet extends HttpServlet {

    private static Logger logger = LoggerFactory.getLogger(FirstServlet.class);

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        logger.info("New Get request with includes");

        getServletContext().getRequestDispatcher("/header.html").include(req, resp);
//        resp.getWriter().println("<p>Response body from servlet</p>");
        getServletContext().getRequestDispatcher("/footer.html").include(req, resp);

//        resp.getWriter().printf("<h1>New GET request with parameters param1 = %s; param2 = %s </h1>",
//                req.getParameter("param1"), req.getParameter("param2"));
//        logger.info("New Get request with redirection");
//        // абсолютная ссылка
//        resp.sendRedirect("https://ya.ru");

    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        logger.info("New POST request");
//        resp.getWriter().printf("<h1>New POST request with body %s</h1>", readAllLines(req.getReader()));
    }

    public String readAllLines(BufferedReader reader) throws IOException {
        StringBuilder content = new StringBuilder();
        String line;

        while ((line = reader.readLine()) != null) {
            content.append(line);
            content.append(System.lineSeparator());
        }

        return content.toString();
    }
}