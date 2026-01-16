/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import ConexionDB.Conexion_Mysql;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author CESAR.OSORIO
 */
@WebServlet(urlPatterns = {"/insrtconclusiones"})
public class insrtConclusiones extends HttpServlet {

    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    Conexion_Mysql conn = new Conexion_Mysql();
    String sql;

    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
       

            String procesado_clave = request.getParameter("idProcesado");
            String fechaResolu;
            String tipoResolu = request.getParameter("tipoConclusion");
            String tipoSobre = request.getParameter("tipoSobreseimto");
            String proceSobre = request.getParameter("proceSobreseimto");
            String procedimiento = request.getParameter("huboProsedimto");
            String tipoProcedimiento = request.getParameter("tipoMedidaPA");
            String privativa = request.getParameter("tipoMedidaPL");
            String noprivativa = request.getParameter("tipoMedidaNPL");
            String internamiento = request.getParameter("internamiento");
            String reparacion = request.getParameter("reparaDanio");
            String tipoRepara = request.getParameter("tipoReparaD");
            String multa = request.getParameter("montoReparaD");
            String impugnacion = request.getParameter("impugnacion");
            String tipoImpugnacion = request.getParameter("tipoImpugnacion");
            String fechaImpugna;
            String personaImpugna = request.getParameter("personaImpugna");
            String comentario = request.getParameter("comentarios");
            if (request.getParameter("fechaReso") != null) {
                fechaResolu = request.getParameter("fechaReso");
            } else {
                fechaResolu = "1899-09-09";
            }
            if (request.getParameter("fechaImpugnacion") != null) {
                fechaImpugna = request.getParameter("fechaImpugnacion");
            } else {
                fechaImpugna = "1899-09-09";
            }
            try {
                conn.Conectar();
                sql = "INSERT INTO DATOS_CONCLUSIONES_ADOJC VALUES (12,12001,1,1,'002/2018-P14','002/2018-C14','" + fechaResolu + "'," + tipoResolu + "," + tipoSobre + "," + proceSobre
                        + "," + procedimiento + "," + tipoProcedimiento + "," + privativa + "," + noprivativa + "," + internamiento + "," + reparacion + "," + tipoRepara
                        + "," + multa + "," + impugnacion + "," + tipoImpugnacion + ",'" + fechaImpugna + "'," + personaImpugna + ",'" + comentario + "',6)";
                System.out.println(sql);
                if (conn.escribir(sql)) {
                    conn.close();
                    response.sendRedirect("elementosPrincipales.jsp?seinserto=si");
                } else {
                    //regresa a insrtTramite y maca error
                    conn.close();
                    response.sendRedirect("conclusiones.jsp?error=si");
                }
            } catch (SQLException ex) {
                Logger.getLogger(insrtTramite.class.getName()).log(Level.SEVERE, null, ex);
            }
        response.setContentType("text/html;charset=UTF-8");
        PrintWriter out = response.getWriter();
        try {
            /* TODO output your page here. You may use following sample code. */
            out.println("<!DOCTYPE html>");
            out.println("<html>");
            out.println("<head>");
            out.println("<title>Servlet insrtconclusiones</title>");
            out.println("</head>");
            out.println("<body>");
            out.println("</body>");
            out.println("</html>");
        } finally {
            out.close();
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
        processRequest(request, response);
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
        processRequest(request, response);
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