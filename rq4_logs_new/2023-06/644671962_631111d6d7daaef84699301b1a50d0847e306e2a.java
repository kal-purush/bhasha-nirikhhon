/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/JSP_Servlet/Servlet.java to edit this template
 */
package Controller;

import DAO.WeekDAO;
import DAO.WeekDAOimplement;
import Entity.WeekCourse;
import Validation.Validator;
import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author HP
 */
public class AddWeekController extends HttpServlet {

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
        try ( PrintWriter out = response.getWriter()) {
            /* TODO output your page here. You may use following sample code. */
            out.println("<!DOCTYPE html>");
            out.println("<html>");
            out.println("<head>");
            out.println("<title>Servlet AddWeekController</title>");
            out.println("</head>");
            out.println("<body>");
            out.println("<h1>Servlet AddWeekController at " + request.getContextPath() + "</h1>");
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
        String stringID = request.getParameter("moid");
        if (Validator.checkIdIsValid(stringID) == true) {
            request.setAttribute("moduleId", stringID);
            request.getRequestDispatcher("AddWeek.jsp").forward(request, response);
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
        String stringID = request.getParameter("moduleId");
        String weeknum = request.getParameter("weeknum");
        String weektitle = request.getParameter("weektitle");
        String weekDes = request.getParameter("weekdescription");
        int moduleId = 0;
        int weeknumInsert = 0;
        if (Validator.checkIdIsValid(stringID) == true) {
            moduleId = Integer.parseInt(stringID);
            if (Validator.checkIdIsValid(weeknum)) {
                weeknumInsert = Integer.parseInt(weeknum);
            }
            WeekDAO dao = new WeekDAOimplement();
            int newId = dao.AddNewWeekCOurse(moduleId, new WeekCourse(0, moduleId, weeknumInsert, weektitle, weekDes));
            if(newId>0){
                response.sendRedirect(request.getContextPath()+"/ModuleDetail?mid="+stringID);
            }

        }
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