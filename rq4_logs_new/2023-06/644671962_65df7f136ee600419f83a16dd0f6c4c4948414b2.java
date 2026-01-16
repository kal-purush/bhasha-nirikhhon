/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/JSP_Servlet/Servlet.java to edit this template
 */
package Controller;

import DAO.WeekDAO;
import DAO.WeekDAOimplement;
import Entity.WeekCourse;
import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author HP
 */
@WebServlet(name = "UpdateWeekController", urlPatterns = {"/UpdateWeek"})
public class UpdateWeekController extends HttpServlet {

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
            out.println("<title>Servlet UpdateWeekController</title>");
            out.println("</head>");
            out.println("<body>");
            out.println("<h1>Servlet UpdateWeekController at " + request.getContextPath() + "</h1>");
            out.println("</body>");
            out.println("</html>");
        }
    }

    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /**
     * Handles the HTTP <code>GET</code> method.
     *
     * @param req servlet request
     * @param resp servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        String stringId = req.getParameter("wid");
        int Id = Validation.Validator.parseValidId(stringId);
        if (Id != 0) {
            WeekDAO dao = new WeekDAOimplement();
            WeekCourse week = dao.getWeekByWId(Id);
            if (week != null) {
                req.setAttribute("weekToUpdate", week);
                req.getRequestDispatcher("WeekUpdate.jsp").forward(req, resp);

            } else {
                // return error page orr showw null valu    e page
            }
        } else {
            // return error page
        }
    }

    /**
     * Handles the HTTP <code>POST</code> method.
     *
     * @param req servlet request
     * @param resp servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        String stringId = req.getParameter("wid");
        String weeknum = req.getParameter("weeknum");
        String weekTitle = req.getParameter("weektitle");
        String weekdes = req.getParameter("weekdescription");
        int Id = Validation.Validator.parseValidId(stringId);
        int weeknumber = Validation.Validator.parseValidId(weeknum);
        if (Id != 0) {
            WeekDAO dao = new WeekDAOimplement();
            WeekCourse weekes = dao.getWeekByWId(Id);
            if (weekes != null) {
                WeekCourse week = new WeekCourse(Id, weekes.getModuleId(), weeknumber, weekTitle, weekdes);

                boolean check = dao.updateWeekById(Id, week);
                if (check==true){
                    resp.sendRedirect(req.getContextPath() + "/ModuleDetail?mid=" + weekes.getModuleId());
                } else {
                    resp.sendRedirect(req.getContextPath() + "/UpdateWeek?wid=" + Id);
                }
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