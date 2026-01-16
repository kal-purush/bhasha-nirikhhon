/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/JSP_Servlet/Servlet.java to edit this template
 */

package control;

import dao.DAO;
import entity.Category;
import entity.Product;
import java.io.IOException;
import java.io.PrintWriter;
import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.List;

/**
 *
 * @author hoan6
 */
@WebServlet(name="CategoryControl", urlPatterns={"/category"})
public class CategoryControl extends HttpServlet {
   
   
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    response.setContentType("text/html;charset=UTF-8");
    String cateID = request.getParameter("cid"); // Lấy category ID từ request
    DAO dao = new DAO();
    
    List<Product> listP; 
    if (cateID == null || cateID.isEmpty()) {
        listP = dao.getAllProduct(); // Nếu không có cid -> lấy tất cả sản phẩm
    } else {
        listP = dao.getProductByCID(cateID); // Lọc theo danh mục
    }
    List<Product> listPP = dao.getAllProduct();
    List<Product> list = dao.getAllProduct();
    List<Category> listCC = dao.getAllCategory(); // Lấy danh sách danh mục
    List<Category> listC = dao.getAllCategory();
    Product last = dao.getLast();
    // Gửi dữ liệu sang JSP
    request.setAttribute("listPP", listPP);
    request.setAttribute("listP", listP);
    request.setAttribute("listCC", listC);
    request.setAttribute("p", last);
    request.setAttribute("tag", cateID);
    request.setAttribute("selectedCid", cateID); // Giữ giá trị danh mục đang chọn

    request.getRequestDispatcher("Category.jsp").forward(request, response);
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
        processRequest(request, response);
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
        processRequest(request, response);
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