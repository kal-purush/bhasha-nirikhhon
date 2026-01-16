package ca.sait.servlets;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author Rehan
 */
public class ShoppingListServlet extends HttpServlet {

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

        String action = request.getParameter("action");
        
        String name = null;

        name = (String) request.getSession().getAttribute("name");

//        if (name != null) {
//            this.getServletContext().getRequestDispatcher("/WEB-INF/shoppingList.jsp").forward(request, response);
//        } else {
//            this.getServletContext().getRequestDispatcher("/WEB-INF/register.jsp").forward(request, response);
//        }
        if (name == null) {
            this.getServletContext().getRequestDispatcher("/WEB-INF/register.jsp").forward(request, response);
        } else {
            this.getServletContext().getRequestDispatcher("/WEB-INF/shoppingList.jsp").forward(request, response);
        }

        if (action != null && action.equals("logout")) {
            request.getSession().invalidate();
            response.sendRedirect("register");

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

        String action = request.getParameter("action");

        switch (action) {
            case "register":
                String name = request.getParameter("name");

                if (name == null || name.equals("")) {
                    String message = "Name cannot be empty!";
                    request.getSession().setAttribute("message", message);
                    this.getServletContext().getRequestDispatcher("/WEB-INF/register.jsp").forward(request, response);

                } else {
                    // Creates session
                    request.getSession().setAttribute("name", name);

                    ArrayList<String> items = new ArrayList<>();

                    if (items.isEmpty()) {
                        String message1 = "There are no items in the list";
                        request.getSession().setAttribute("message1", message1);
                    }

                    request.getSession().setAttribute("items", items);
                }
                break;

            case "add": {
                String item = request.getParameter("item");
                // Adds items
                ArrayList<String> items = (ArrayList<String>) request.getSession().getAttribute("items");
                if (items.isEmpty()) {
                    String message1 = "There are no items in the list";
                    request.getSession().setAttribute("message1", message1);
                }
                if (item.equals("")) {
                    String message2 = "Item cannot be empty!";
                    request.getSession().setAttribute("message2", message2);
                } else {
                    items.add(item);
                    String message1 = "";
                    request.getSession().setAttribute("message1", message1);
                    String message2 = "Item added successfully!";
                    request.getSession().setAttribute("message2", message2);
                }
                request.getSession().setAttribute("items", items);
                break;
            }
            case "delete": {
                String itemValue = request.getParameter("item");
                ArrayList<String> items = (ArrayList<String>) request.getSession().getAttribute("items");
                if (items.isEmpty()) {
                    String message1 = "There are no items in the list";
                    request.getSession().setAttribute("message1", message1);
                    String message2 = "Cannot delete item if the list is empty!";
                    request.getSession().setAttribute("message2", message2);
                } else if (itemValue == null) {
                    String message2 = "Must select an item before deleting!";
                    request.getSession().setAttribute("message2", message2);
                } else {
                    items.remove(itemValue);
                    String message2 = "Item removed successfully!";
                    request.getSession().setAttribute("message2", message2);
                }
                if (items.isEmpty()) {
                    String message1 = "There are no items in the list";
                    request.getSession().setAttribute("message1", message1);
                }
                request.getSession().setAttribute("items", items);
                break;
            }
            default:
                break;
        }

        this.getServletContext().getRequestDispatcher("/WEB-INF/shoppingList.jsp").forward(request, response);

    }

}