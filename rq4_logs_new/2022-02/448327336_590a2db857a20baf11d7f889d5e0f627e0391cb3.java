/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Controller.Authen_Author;

import DAO.Account.AccountDAO;
import Model.Account;
import Utils.FacebookLoginUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.restfb.types.User;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author ADMIN
 */
public class LoginFacebook extends HttpServlet {

    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/html;charset=UTF-8");
        try (PrintWriter out = response.getWriter()) {
            /* TODO output your page here. You may use following sample code. */
            out.println("<!DOCTYPE html>");
            out.println("<html>");
            out.println("<head>");
            out.println("<title>Servlet LoginFacebook</title>");
            out.println("</head>");
            out.println("<body>");
            out.println("<h1>Servlet LoginFacebook at " + request.getContextPath() + "</h1>");
            out.println("</body>");
            out.println("</html>");
        }
    }

    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /**
     * Handles the HTTP <code>GET</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String code = request.getParameter("code");

        if (code == null || code.isEmpty()) {
            RequestDispatcher dis = request.getRequestDispatcher("login.jsp");
            dis.forward(request, response);
        } else {
            String accessToken = FacebookLoginUtils.getToken(code);
            User user = FacebookLoginUtils.getUserInfo(accessToken);

            int createState = -1;
            Account currentAccount = null;
            try {
                if (!AccountDAO.isHaveUserName(user.getId())) {
                    createState = AccountDAO.createAccount(user.getId(), user.getId(), user.getEmail());
                    if (createState == 0) {
                        String imageUrl = "http://graph.facebook.com/" + user.getId() + "/picture?type=large&redirect=true&width=500&height=500";
                        AccountDAO.updateUserInfo(user.getId(), user.getEmail(), user.getName(), user.getLocale(), imageUrl, user.getLink());
                    }
                }
                currentAccount = AccountDAO.getAccountByUserName(user.getId());
            } catch (SQLException ex) {
                Logger.getLogger(LoginGoogle.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println(accessToken);
            request.getSession().setAttribute("currentAccount", currentAccount);
            request.setAttribute("isLoginFacebook", true);

            ObjectMapper mapper = new ObjectMapper();
//            ObjectNode rootNode = mapper.createObjectNode();
//            rootNode.put("currentAccount", currentAccount.toString());

            String dataReturn = mapper.writeValueAsString(currentAccount);

            request.setAttribute("currentAccount", dataReturn);
            RequestDispatcher dis = request.getRequestDispatcher("index.jsp");
            dis.forward(request, response);
        }
    }

    /**
     * Handles the HTTP <code>POST</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Returns a short description of the servlet.
     *
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>

}