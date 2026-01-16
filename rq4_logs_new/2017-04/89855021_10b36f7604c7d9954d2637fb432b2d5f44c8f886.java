package servlet;

import dao.ItemDao;
import model.Order;
import model.OrderItem;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mario on 28.04.2017.
 */
@WebServlet(name = "AddingServlet", urlPatterns = "/adding")
public class AddingServlet extends HttpServlet {
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
//        response.getWriter().println("dodawanie produktu do koszyka" );


//        Order order;
//
//        if(request.getSession().getAttribute("order") == null) {
//            order = new Order();
//            request.getSession().setAttribute("order",order);
//        }
//        else {
//            order = (Order) request.getSession().getAttribute("order");
//        }

        ArrayList<OrderItem> orderItems;
        if (request.getSession().getAttribute("orderItems") == null) {
            orderItems = new ArrayList<>();

            addItem(request, orderItems);

            request.getSession().setAttribute("orderItems", orderItems);
            response.sendRedirect(request.getContextPath() + "/shop");

        }
        else {
            orderItems = (ArrayList<OrderItem>) request.getSession().getAttribute("orderItems");
            int itemId = Integer.parseInt(request.getParameter("id"));
            try{
            for (OrderItem oi : orderItems) {
                if (oi.getItem_id() == itemId) {
                    oi.setQuantity(oi.getQuantity() + Integer.parseInt(request.getParameter("quantity")));

                } else
                    addItem(request, orderItems);
            }
            }catch(Exception e){
                e.printStackTrace();
            }
                request.getSession().setAttribute("orderItems", orderItems);

                response.sendRedirect(request.getContextPath() + "/shop");
            }


        }

    public static void addItem(HttpServletRequest request,List<OrderItem> orderItems){
        OrderItem orderItem = new OrderItem();
        orderItem.setProducent(request.getParameter("producent"));
        orderItem.setModel(request.getParameter("model"));
        orderItem.setItem_id(Integer.parseInt(request.getParameter("id")));
        orderItem.setPrice(Double.parseDouble(request.getParameter("price")));
        orderItem.setQuantity(Integer.parseInt(request.getParameter("quantity")));
        orderItems.add(orderItem);
    }
}