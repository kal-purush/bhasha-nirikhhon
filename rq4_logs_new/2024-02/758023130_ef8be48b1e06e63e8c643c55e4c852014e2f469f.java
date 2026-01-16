/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/JSP_Servlet/Servlet.java to edit this template
 */

package Controllers;

import DAOs.AccountDAO;
import DAOs.StaffDAO;
import Models.Account;
import Models.Staff;
import java.io.IOException;
import java.io.PrintWriter;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import java.sql.Date;
import java.text.SimpleDateFormat;

/**
 *
 * @author Admin
 */
public class StaffController extends HttpServlet {
   
    /** 
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code> methods.
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
            out.println("<title>Servlet StaffController</title>");  
            out.println("</head>");
            out.println("<body>");
            out.println("<h1>Servlet StaffController at " + request.getContextPath () + "</h1>");
            out.println("</body>");
            out.println("</html>");
        }
    } 

    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /** 
     * Handles the HTTP <code>GET</code> method.
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
        String url = request.getRequestURI();
        StaffDAO sdao = new StaffDAO();
        
        if(url.endsWith("/StaffController")){
            request.getRequestDispatcher("AddnewStaff.jsp").forward(request, response);
        } else if(url.startsWith("/StaffController/DetailStaff")){
            String[] s = url.split("/");
            int staffId = Integer.parseInt(s[s.length - 1]);
            Staff staff = sdao.getStaffById(staffId);
            
            HttpSession session = request.getSession();
            session.setAttribute("getStaff", staff);
            request.getRequestDispatcher("/DetailStaff.jsp").forward(request, response);
        } else if(url.startsWith("/StaffController/EditStaff")){
            String[] s = url.split("/");
            int staffId = Integer.parseInt(s[s.length - 1]);
            Staff staff = sdao.getStaffById(staffId);
            
            HttpSession session = request.getSession();
            session.setAttribute("getStaff", staff);
            request.getRequestDispatcher("/EditStaff.jsp").forward(request, response);
        } else if(url.startsWith("/StaffController/DeleteStaff")){
            String[] s = url.split("/");
            int staffId = Integer.parseInt(s[s.length - 1]);
            Staff staff = sdao.getStaffById(staffId);
            
            HttpSession session = request.getSession();
            session.setAttribute("getStaff", staff);
            request.getRequestDispatcher("/DeleteStaff.jsp").forward(request, response);
        }
    } 

    /** 
     * Handles the HTTP <code>POST</code> method.
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
        StaffDAO sdao = new StaffDAO();
        AccountDAO adao = new AccountDAO();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        
        if (request.getParameter("btnAddStaff") != null) {
            
            String username = request.getParameter("username");
            String password = request.getParameter("password");
            String fullname = request.getParameter("fullname");
            String phone = request.getParameter("phone");
            String email = request.getParameter("email");
            Date birthday = Date.valueOf(request.getParameter("birthday"));
            String gender = request.getParameter("gender");
            String address = request.getParameter("address");
            String position = request.getParameter("position");
            Date begin_work = Date.valueOf(request.getParameter("begin_work"));
            
            Account acc = new Account(0,username, password, fullname, phone, email, 0, 0);
            int acc_id = adao.createAcc(acc);

            if(acc_id != 0){
                Staff staff = new Staff(0, acc_id, acc.getUsername(), acc.getPassword(), acc.getFullname(), acc.getPhone_number(), acc.getEmail(), birthday, gender, address, position, begin_work, 0, 0);
                Staff new_staff = sdao.addNewStaff(staff);
                
                if(new_staff != null){
                    response.sendRedirect("/AdminController/adminListStaff");
                } else{
                    response.sendRedirect("/StaffController");
                }
            }
        }
        
        if(request.getParameter("btnEditStaff") != null){
            int staff_id = Integer.parseInt(request.getParameter("idStaff"));
            int acc_id = Integer.parseInt(request.getParameter("idAcc"));
            
            String username = request.getParameter("username");
            String password = request.getParameter("password");
            String fullname = request.getParameter("fullname");
            String phone = request.getParameter("phone");
            String email = request.getParameter("email");
            Date birthday = Date.valueOf(request.getParameter("birthday"));
            String gender = request.getParameter("gender");
            String address = request.getParameter("address");
            String position = request.getParameter("position");
            Date begin_work = Date.valueOf(request.getParameter("begin_work"));
            
            Staff staff = new Staff(username, password, fullname, phone, email, birthday, gender, address, position, begin_work);
            Staff new_staff = sdao.editStaff(staff_id, staff);
            
            if(new_staff != null){
                Account acc = new Account(username, password, fullname, phone, email);
                Account new_acc = adao.updateAccStaff(acc_id, acc);
                
                response.sendRedirect("/AdminController/adminListStaff");
            } else{
                response.sendRedirect("/StaffController/EditStaff/" + staff_id);
            }
        }
        
        if(request.getParameter("btnDeleteStaff") != null){
            int staff_id = Integer.parseInt(request.getParameter("idStaff"));
            int acc_id = Integer.parseInt(request.getParameter("idAcc"));
            long millis = System.currentTimeMillis();   
            java.sql.Date current = new java.sql.Date(millis);
       
            if(sdao.isDelete(staff_id, current)){
                if(adao.isDelete(acc_id, current)){
                    response.sendRedirect("/AdminController/adminListStaff");
                }
            } else{
                response.sendRedirect("/StaffController/DeleteStaff/" + staff_id);
            }
        }
    }

    /** 
     * Returns a short description of the servlet.
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>

}