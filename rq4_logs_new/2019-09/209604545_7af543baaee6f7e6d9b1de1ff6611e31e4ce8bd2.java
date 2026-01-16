package club.piclight.lightmonitor.club.piclight.lightmonitor.NetAPI;

import club.piclight.lightmonitor.Bean.ClientBean;
import club.piclight.lightmonitor.DAO.ClientsDAO;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;

@WebServlet(urlPatterns = "/heartbeart")
public class Heartbeart extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        req.setCharacterEncoding("UTF-8");
        resp.setCharacterEncoding("UTF-8");

        String userSession = null;
        int id;
        userSession = req.getParameter("usersession");
        if (userSession != null) {
            id = SessionGetUser(userSession);
            if (id != -1) {
                ClientsDAO.getClientsDao().refreshClient(id, req.getRemoteHost());
                resp.setStatus(200);
                resp.setContentType("text/plain");

                ServletOutputStream out = resp.getOutputStream();
                out.println("OK! Refresh LastestOnline Time.");
            }
        } else {
            resp.setStatus(400);
            resp.setContentType("text/plain");

            ServletOutputStream out = resp.getOutputStream();
            out.println("Error! User No found.");
        }
    }


    /**
     * Use Usersession to search user id
     *
     * @param session
     * @return
     */
    private int SessionGetUser(String session) {
        ArrayList<ClientBean> clientBeans = ClientsDAO.getClientsDao().getClientList();
        for (ClientBean client : clientBeans) {
            if (session.equals(client.getUserSession()))
                return client.getId();
        }
        return -1; //Can't found user by this session
    }
}