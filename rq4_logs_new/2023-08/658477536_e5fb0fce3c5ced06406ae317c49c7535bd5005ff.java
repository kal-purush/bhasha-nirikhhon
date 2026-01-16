package by.it.medved.controllers.menu;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import static by.it.medved.util.FieldsEntities.*;
import static by.it.medved.util.FieldsEntities.EXIT;
import static by.it.medved.util.Link.*;

@WebServlet(urlPatterns = "/managers/menu")
public class ManagerMenuController extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        switch (req.getParameter(ACTION)) {
            case SHOW_USERS:
                req.getRequestDispatcher(USERS_READ_URI).forward(req, resp);
                break;
            case EDIT_PROFILE:
                req.getRequestDispatcher(USER_EDIT_PAGE).forward(req, resp);
                break;
            case CHANGE_PASSWORD:
                req.getRequestDispatcher(CHANGE_PASSWORD_PAGE).forward(req, resp);
                break;
            case SHOW_TICKETS:
                req.getRequestDispatcher(TICKETS_READ_URI).forward(req, resp);
                break;
            case SHOW_MOVIES:
                req.getRequestDispatcher(MOVIES_URI).forward(req, resp);
                break;
            case EXIT:
                req.getRequestDispatcher(EXIT_URI).forward(req, resp);
                break;
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        req.getRequestDispatcher(MANAGER_MENU_PAGE).forward(req, resp);
    }
}