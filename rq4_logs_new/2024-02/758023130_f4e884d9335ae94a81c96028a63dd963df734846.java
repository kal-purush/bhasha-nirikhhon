/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/JSP_Servlet/Servlet.java to edit this template
 */

package Controllers;

import DAOs.CustomerDAO;
import DAOs.OrderDAO;
import DAOs.OrderDetailDAO;
import DAOs.OrderStatusHistoryDAO;
import Models.Customer;
import Models.Order;
import Models.OrderDetail;
import Models.OrderStatusHistory;
import Models.Staff;
import java.io.IOException;
import java.io.PrintWriter;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.sql.Date;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;

/**
 *
 * @author PC
 */
public class OrderController extends HttpServlet {
   
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
            out.println("<title>Servlet OrderController</title>");  
            out.println("</head>");
            out.println("<body>");
            out.println("<h1>Servlet OrderController at " + request.getContextPath () + "</h1>");
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
        String path = request.getRequestURI();
        
        if (path.startsWith("/OrderController/OrderDetailAdmin/")) {
            String s[] = path.split("/");
            int orderID = Integer.parseInt(s[s.length - 1]);
            OrderDAO oDAO = new OrderDAO();
            OrderDetailDAO odDAO = new OrderDetailDAO();
            CustomerDAO cuDAO = new CustomerDAO();

            Order getOrderByID = oDAO.getOrderByID(orderID);
            LinkedList<OrderDetail> odList = odDAO.getAllOrderDetailsByOrderID(orderID);
            Customer cus = cuDAO.getCustomerByCusID(getOrderByID.getCus_id());

            request.setAttribute("getOrderByID", getOrderByID);
            request.setAttribute("odList", odList);
            request.setAttribute("cus", cus);
            request.getRequestDispatcher("/orderDetailAdmin.jsp").forward(request, response);
        } else if (path.startsWith("/OrderController/UpdateOrderAdmin/")) {
            String s[] = path.split("/");
            int orderID = Integer.parseInt(s[s.length - 1]);
            OrderDAO oDAO = new OrderDAO();
            OrderDetailDAO odDAO = new OrderDetailDAO();
            CustomerDAO cuDAO = new CustomerDAO();

            Order getOrderByID = oDAO.getOrderByID(orderID);
            LinkedList<OrderDetail> odList = odDAO.getAllOrderDetailsByOrderID(orderID);
            Customer cus = cuDAO.getCustomerByCusID(getOrderByID.getCus_id());

            request.setAttribute("getOrderByID", getOrderByID);
            request.setAttribute("odList", odList);
            request.setAttribute("cus", cus);
            request.getRequestDispatcher("/EditOrderForm.jsp").forward(request, response);
        } else if (path.startsWith("/OrderController/DeleteOrderAdmin/")) {
            String s[] = path.split("/");
            int orderID = Integer.parseInt(s[s.length - 1]);
            OrderDAO oDAO = new OrderDAO();
            OrderDetailDAO odDAO = new OrderDetailDAO();
            CustomerDAO cuDAO = new CustomerDAO();

            Order getOrderByID = oDAO.getOrderByID(orderID);
            LinkedList<OrderDetail> odList = odDAO.getAllOrderDetailsByOrderID(orderID);
            Customer cus = cuDAO.getCustomerByCusID(getOrderByID.getCus_id());

            request.setAttribute("getOrderByID", getOrderByID);
            request.setAttribute("odList", odList);
            request.setAttribute("cus", cus);
            request.getRequestDispatcher("/DeleteOrderForm.jsp").forward(request, response);
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
        
        // Cap nhat trang thai don hang
        if (request.getParameter("updateOrderAdminBtn") != null) {
            OrderDAO oDAO = new OrderDAO();
            OrderStatusHistoryDAO oshDAO = new OrderStatusHistoryDAO();

            int orderID = Integer.parseInt(request.getParameter("orderID"));
            int staffID = Integer.parseInt(request.getParameter("staffID"));
            String statusOrder = request.getParameter("statusBtn");
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            LocalDateTime now = LocalDateTime.now();
            Date orderStatusDate = Date.valueOf(dtf.format(now));

            Order getOrder = oDAO.getOrderByID(orderID);
            Order orderEdit = new Order(0, 0, "", "", statusOrder, getOrder.getO_date(), 0, 0);
            int editOrderStatus = oDAO.editOrderStatus(orderID, orderEdit);

            if (!getOrder.getStatus().equals(statusOrder)) {
                OrderStatusHistory ordHis = new OrderStatusHistory(0, orderID, staffID, statusOrder, "Cập nhật", orderStatusDate);
                int addHisOrderStatus = oshDAO.addOrderStatus(ordHis);
            }

            if (editOrderStatus > 0) {
                response.sendRedirect("/AdminController/adminListOrder");
            } else {
                response.sendRedirect("/OrderController/UpdateOrderAdmin/" + orderID);
            }
        }
        
        // Xoa don hang trong admin
        if (request.getParameter("deleteOrderBtn") != null) {
            OrderDAO oDAO = new OrderDAO();
            OrderStatusHistoryDAO oshDAO = new OrderStatusHistoryDAO();

            int orderID = Integer.parseInt(request.getParameter("orderID"));
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            LocalDateTime now = LocalDateTime.now();
            Date orderStatusDate = Date.valueOf(dtf.format(now));
            Staff getStaff = (Staff) request.getSession().getAttribute("staff");
            String getStatus = request.getParameter("statusWhenDelete");

            int deleteOrder = oDAO.deleteOrder(orderID);

            OrderStatusHistory ordHis = new OrderStatusHistory(0, orderID, getStaff.getStaff_id(), getStatus, "Xóa", orderStatusDate);
            int addHisOrderStatus = oshDAO.addOrderStatus(ordHis);
            if (deleteOrder > 0) {
                response.sendRedirect("/AdminController/adminListOrder");
            } else {
                response.sendRedirect("/OrderController/DeleteOrderAdmin/" + orderID);
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