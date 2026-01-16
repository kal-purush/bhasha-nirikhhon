package by.it.medved.controllers.ticket;

import by.it.medved.entities.Ticket;
import by.it.medved.entities.User;
import org.hibernate.Hibernate;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.util.List;

import static by.it.medved.util.FieldsEntities.*;
import static by.it.medved.util.Link.AUTHORIZATION_CONTROLLER_URI;
import static by.it.medved.util.Link.TICKETS_PAGE;
import static by.it.medved.util.Message.TICKETS_EMPTY;

@WebServlet(urlPatterns = "/ticket/read")
public class ReadTicketsController extends HttpServlet {


    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        HttpSession session = req.getSession();
        User user = (User) session.getAttribute(USER);
        List<Ticket> tickets = user.getTickets();
        if (!tickets.isEmpty()) {
            req.setAttribute(TICKETS, tickets);
            req.getRequestDispatcher(TICKETS_PAGE).forward(req, resp);
        } else {
            req.setAttribute(NO_TICKETS, TICKETS_EMPTY);
            req.getRequestDispatcher(AUTHORIZATION_CONTROLLER_URI).forward(req, resp);
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }
}