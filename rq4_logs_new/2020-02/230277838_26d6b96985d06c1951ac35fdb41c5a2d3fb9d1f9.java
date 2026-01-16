package ru.epam.javacore.lesson_25_web.lesson.controller;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public abstract class BaseController extends HttpServlet {

    protected void goToPage(
            HttpServletRequest servletRequest,
            HttpServletResponse servletResponse,
            String dest) throws ServletException, IOException {

        RequestDispatcher dispatcher = servletRequest
                .getRequestDispatcher("/WEB-INF/jsp/" + dest);

        dispatcher.forward(servletRequest, servletResponse);

    }
}