package Controller;

/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/JSP_Servlet/Servlet.java to edit this template
 */
import DAO.ModuleDAO;
import DAO.ModuleDAOimplement;
import DAO.WeekDAO;
import DAO.WeekDAOimplement;
import Entity.WeekCourse;
import Validation.Validator;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author HP
 */
@WebServlet(urlPatterns = {"/UpdateModule"})
public class UpdateModuleController extends HttpServlet  {

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
            out.println("<title>Servlet UpdateModuleController</title>");
            out.println("</head>");
            out.println("<body>");
            out.println("<h1>Servlet UpdateModuleController at " + request.getContextPath() + "</h1>");
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
        String stringId = request.getParameter("mid");
        
        Validator val = new Validator();
        if (val.checkIdIsValid(stringId)) {
            ModuleDAO dao = new ModuleDAOimplement();
            int id = Integer.parseInt(stringId);
            Entity.Module module = dao.getModuleOfCourseByMid(id);
            request.setAttribute("oldmodule", module);

            request.getRequestDispatcher("UpdateModule.jsp").forward(request, response);
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
        String stringId = request.getParameter("moduleId");
        String cstringId = request.getParameter("courseID");
        String name = request.getParameter("moduleName");
        String des = request.getParameter("modulecourdescription");
        int id = 0;
        if (stringId != null) {
            id = Integer.parseInt(stringId);
        }

        Entity.Module newModule = new Entity.Module(id, 0, name, des);
        ModuleDAO dao = new ModuleDAOimplement();

        if (newModule != null) {
            boolean flag = dao.updateModuleByModuleId(id, newModule);
            if (flag) {
                response.sendRedirect(request.getContextPath() + "/CourseDetail?id=" + cstringId);
            } else {
                response.sendRedirect(request.getContextPath() + "/UpdateModule?mid="+stringId);
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