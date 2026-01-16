/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Control_Seciones;

import Control_BD.ControlUsuarios;
import Control_validaciones.Checador;
import Entidades.Usuario;
import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author Zush18
 */
public class Registrar extends HttpServlet {

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
        try(PrintWriter out = response.getWriter()){
            
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
        try(PrintWriter out = response.getWriter()){
            out.println("<!DOCTYPE html><html><head><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n<link href=\"//fonts.googleapis.com/css?family=Raleway:400,300,600\" rel=\"stylesheet\" type=\"text/css\">\n<link rel=\"stylesheet\" href=\"css/normalize.css\">\n<link rel=\"stylesheet\" href=\"css/skeleton.css\"><title>DeepWeb</title></head><body>");  
            Checador validar = new Checador();
            ControlUsuarios cu = new ControlUsuarios();
            String nom = null;
            String contra = null;
            try{
                nom = request.getParameter("usuarioNew");
                contra = request.getParameter("contraNew");
            }catch(Exception e){
                System.out.println("Error "+e.getMessage());
            }
            if(validar.usuario(nom)){
                out.println("<h1>Usuario "+validar.getMensaje()+"</h1>");
            }
            if(validar.contraseña(contra)){
                out.println("<h1>Contraseña "+validar.getMensaje()+"</h1>");
            }
            if(validar.usuario(nom) && validar.contraseña(contra)){
                int status = 400;
                request.setAttribute("status", status);
                response.sendRedirect("Error.jsp");
            }
            if (!validar.usuario(nom) && !validar.contraseña(contra)) {
                Usuario usu = new Usuario(nom, contra);
                int agregarUsuario = cu.agregarUsuario(usu, 0);
                if(agregarUsuario > 0){
                    out.println("<h1>Agragado correctamente</h1>");
                }else{
                    out.println("<h1>No agregado</h1>");
                }
            }
        }
    }

}