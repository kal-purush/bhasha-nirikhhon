/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.daw.clases;

import com.daw.films.dao.PeliculaDAO;
import com.daw.films.dao.PeliculaDAOJDBC;
import com.daw.films.dao.PeliculaDAOList;
import com.daw.films.model.Pelicula;
import com.daw.util.Util;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletConfig;
import javax.servlet.annotation.HttpConstraint;
import javax.servlet.annotation.ServletSecurity;
import javax.swing.JOptionPane;
import sun.rmi.runtime.Log;

/**
 *
 * @author Carlos y Susana
 */
@WebServlet(name = "AdminControlador", urlPatterns = {"/AdminControlador/*"})
//@ServletSecurity(@HttpConstraint(rolesAllowed={"ADMINISTRADORES"}))
public class AdminControlador extends HttpServlet {

    private PeliculaDAO peliculaDAO;
    private String action;
    private String srvUrl;
    private String imgUrl;
    private final String srvViewPath = "/WEB-INF/Vistas/admin/";

    @Override
    public void init(ServletConfig servletConfig) throws ServletException {
        super.init(servletConfig); //To change body of generated methods, choose Tools | Templates.

        //Initialize Model Data
        peliculaDAO = new PeliculaDAOJDBC();
        srvUrl = servletConfig.getServletContext().getContextPath() + "/AdminControlador";
        imgUrl = servletConfig.getServletContext().getContextPath() + "/imagenes";

    }

    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        response.setContentType("text/html;charset=UTF-8");
        response.setHeader("Expires", "0");

        action = (request.getPathInfo() != null ? request.getPathInfo() : "");
        request.setAttribute("srvUrl", srvUrl);
        request.setAttribute("imgUrl", imgUrl);
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
        processRequest(request, response);
        RequestDispatcher rd;

        switch (action) {
            case "/Inicio": {
                rd = request.getRequestDispatcher(srvViewPath + "Inicio.jsp");
                rd.forward(request, response);
                break;

            }
            case "/Peliculas": {

                List<Pelicula> lp = peliculaDAO.buscaTodos();
                request.setAttribute("peliculas", lp);
                rd = request.getRequestDispatcher(srvViewPath + "Peliculas.jsp");
                rd.forward(request, response);
                break;
            }
            case "/verFicha": {

                int id = Integer.parseInt(request.getParameter("id"));
                Pelicula p = peliculaDAO.buscaId(id);
                request.setAttribute("pelicula", p);
                rd = request.getRequestDispatcher(srvViewPath + "Ficha.jsp");
                rd.forward(request, response);
                break;

            }
            case "/editarPelicula": {
                Pelicula p;
                int id = Integer.parseInt(Util.getParam(request.getParameter("id"), "0"));
                p = peliculaDAO.buscaId(id);
                request.setAttribute("pelicula", p);
                rd = request.getRequestDispatcher(srvViewPath + "editarPelicula.jsp");
                rd.forward(request, response);
                break;
            }
            case "/borra": {
                int id = Integer.parseInt(Util.getParam(request.getParameter("id"), "0"));
                int iden = id;
                if (id > 0) {
                    peliculaDAO.borra(id);
                }
                response.sendRedirect(srvUrl);
                return;
            }
            case "/addPelicula": {
                Pelicula p = new Pelicula();
                request.setAttribute("pelicula", p);
                rd = request.getRequestDispatcher(srvViewPath + "addPelicula.jsp");
                rd.forward(request, response);
                break;
            }

            default: {

                List<Pelicula> lp = peliculaDAO.buscaTodos();
                request.setAttribute("peliculas", lp);
                rd = request.getRequestDispatcher(srvViewPath + "Inicio.jsp");
                rd.forward(request, response);
                break;

            }

        }

    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        processRequest(request, response);
        RequestDispatcher rd;

        //Log.log(Level.INFO, "Peticion POST al administrador {0}", action);
        switch (action) {

            case "/addPelicula": {
                Pelicula p = new Pelicula();
                if (validarPelicula(request, p)) {
                    peliculaDAO.crea(p);
                    response.sendRedirect(srvUrl + "/Ficha?id=" + p.getId());
                } else {
                    request.setAttribute("pelicula", p);
                    rd = request.getRequestDispatcher(srvViewPath + "/addPelicula.jsp");
                    rd.forward(request, response);
                }
                break;
            }

            case "/busca": {
                Pelicula p = new Pelicula();
                String busca = Util.getParam(request.getParameter("buscar"), "");
                if (busca != "") {
                    p = peliculaDAO.buscaTitulo(busca);
                    request.setAttribute("pelicula", p);
                    rd = request.getRequestDispatcher(srvViewPath + "Peliculas.jsp");
                    rd.forward(request, response);
                }
                break;
            }

            case "/editarPelicula": {
                Pelicula p = new Pelicula();
                if (validarPelicula(request, p)) {
                    peliculaDAO.guarda(p);
                    response.sendRedirect(srvUrl);
                } else {
                    request.setAttribute("pelicula", p);
                    rd = request.getRequestDispatcher(srvViewPath + "/editarPelicula.jsp");
                    rd.forward(request, response);
                }
                break;
            }
            default: {
                response.sendRedirect(srvUrl);
                break;
            }
        }
    }

    /**
     * Returns a short description of the servlet.
     *
     * @return a String containing servlet description
     */
    private boolean validarPelicula(HttpServletRequest request, Pelicula p) {

        boolean valido = true;

        int id = Integer.parseInt(Util.getParam(request.getParameter("id"), "0"));
        String titulo = Util.getParam(request.getParameter("titulo"), "");
        String sinopsis = (String) request.getParameter("sinopsis");

        p.setId(id);
        p.setTitulo(titulo);
        p.setSinopsis(sinopsis);

        if (titulo.length() <= 0) {
            request.setAttribute("errNombre", "Nombre no válido");
            //Log.log(Level.INFO,"Enviado nombre de usuario no válido");
            valido = false;
        }

        if (sinopsis.length() <= 0) {
            request.setAttribute("errSinopsis", "Sinopsis no válida");
            //Log.log(Level.INFO,"Enviado nombre de usuario no válido");
            valido = false;
        }

        return valido;

    }

    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>

}