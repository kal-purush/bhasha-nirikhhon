package br.com.perritoCaliente.servlet;

import java.io.IOException;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet("/recipe-description")
public class RecipeDescriptionServlet extends HttpServlet {
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        request.setCharacterEncoding("UTF-8");
        String idReceitaStr = request.getParameter("idReceita");

        if (idReceitaStr != null) {
            int idReceita = Integer.parseInt(idReceitaStr);

            request.setAttribute("idReceita", idReceita);

            RequestDispatcher dispatcher = request.getRequestDispatcher("/recipeDescription.jsp");
            dispatcher.forward(request, response);
        } else {
            response.sendRedirect("recipes.jsp");
        }
    }
}