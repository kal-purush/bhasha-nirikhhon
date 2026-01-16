package helloPackage.servlets;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;

/**
 * Created by AjaxSurangama on 2/12/2017.
 */
public class SetLoginEmailServlet extends HttpServlet {

    @Override
    public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String userEmail =req.getParameter("loginemail");
        HttpSession session = req.getSession();
        session.setAttribute("loginemail",userEmail );
        System.out.println(req.getParameter("loginemail"));
        resp.sendRedirect("lists_all?user="+userEmail);
    }
}