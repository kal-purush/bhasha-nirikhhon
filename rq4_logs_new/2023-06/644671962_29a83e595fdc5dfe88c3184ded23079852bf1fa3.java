/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/JSP_Servlet/Servlet.java to edit this template
 */
package Controller;

import DAO.QuizDAO;
import DAO.QuizDAOimplement;
import Entity.Quiz;
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
@WebServlet(name = "AddQuizController", urlPatterns = {"/AddQuiz"})
public class AddQuizController extends HttpServlet {

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
            out.println("<title>Servlet AddQuizController</title>");
            out.println("</head>");
            out.println("<body>");
            out.println("<h1>Servlet AddQuizController at " + request.getContextPath() + "</h1>");
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
        String stringId = request.getParameter("wid");
        QuizDAO dao = new QuizDAOimplement();
        int id = Validation.Validator.parseValidId(stringId);
        List<Quiz> quizs = dao.getAllQuizbyWeekId(id);
        request.setAttribute("quizs", quizs);
        request.setAttribute("wid", stringId);
        request.getRequestDispatcher("AddQuiz.jsp").forward(request, response);

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
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        String widString = req.getParameter("weekid");
        String qNameString = req.getParameter("QName");
        String qTimeString = req.getParameter("Qtime");
        String qTopicString = req.getParameter("Qtopic");
        int qTime = Validation.Validator.parseValidId(qTimeString);
        int wId = Validation.Validator.parseValidId(widString);
        Quiz quiz = new Quiz(0, qNameString, qTopicString, qTime, wId);
        System.out.println("quiz:"+quiz);  
        if (quiz != null) {
            QuizDAO dao = new QuizDAOimplement();
            int idgen = dao.AddnewQuiz(quiz);
            if (idgen > 0) {
                resp.sendRedirect(req.getContextPath() + "/AddQuiz?wid=" + wId);
                
                System.out.println("quiz:"+idgen); 
            }else{
                System.out.println("quiz:"+quiz);   
                System.out.println("quiz:"+idgen); 
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