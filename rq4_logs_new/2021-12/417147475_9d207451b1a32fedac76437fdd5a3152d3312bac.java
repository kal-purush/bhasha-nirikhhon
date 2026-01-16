package by.dutov.jee.controllers.servlets.admin;

import by.dutov.jee.service.DeleteService;
import by.dutov.jee.service.Finance;
import by.dutov.jee.utils.CommandServletUtils;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.DispatcherType;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Slf4j
@WebServlet("/admin/delete-user")
public class DeleteUserServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        log.info("Entered Delete User Page");
        CommandServletUtils.dispatcher(req, resp, "/admin/deleteUserPage.jsp", DispatcherType.FORWARD);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        log.info("Get parameters");
        String userName = req.getParameter("userName");
        log.info("userName = {}", userName);
        log.info("Get person from db");
        DeleteService.getInstance().deleteUser(req, resp, userName);
//        CommandServletUtils.dispatcher(req, resp, "/admin/deleteUserPage.jsp", DispatcherType.INCLUDE);
    }
}