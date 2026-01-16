/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import ConexionDB.Conexion_Mysql;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

/**
 *
 * @author FERMIN.GOMEZ
 */
@WebServlet(urlPatterns = {"/insrtDelitos"})
public class insrtDelitos extends HttpServlet {

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
    ResultSet rs;
    
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        
        HttpSession sesion= request.getSession();
        
        String entidad =(String) sesion.getAttribute("entidad");
        String municipio =(String) sesion.getAttribute("municipio");
        String distrito =(String) sesion.getAttribute("distrito");
        String numero =(String) sesion.getAttribute("numero");
        String jConcatenado =entidad+municipio+distrito+numero;
        String expediente =(String) sesion.getAttribute("expediente");
        
        String delitoCP=request.getParameter("delitoCP");
        String articuloCP=request.getParameter("articuloCP");
        String delitoNT=request.getParameter("delitoNT");
        String fuero=request.getParameter("fuero");
        String reclasificaDel=request.getParameter("reclasificaDel");
        String fechaReclaDel=request.getParameter("fechaReclaDel");
        String consumacion=request.getParameter("consumacion");
        String calificacion=request.getParameter("calificacion");
        String concurso=request.getParameter("concurso");
        String clasificacion=request.getParameter("clasificacion");
        String comision=request.getParameter("comision");
        String accion=request.getParameter("accion");
        String modalidad=request.getParameter("modalidad");
        String instrumentos=request.getParameter("instrumentos");
        String ocurrencia=request.getParameter("ocurrencia");
        String entidadD=request.getParameter("entidadD");
        String municipioD=request.getParameter("municipioD");
        String numAdo=request.getParameter("numAdo");
        String numVic=request.getParameter("numVic");
        String comentarios=request.getParameter("comentarios");
        if (ocurrencia == null) {
            ocurrencia = "1899-09-09";
        }
        try {
            String delitoClave = generaDelitoClave(expediente) + 1;
            conn.Conectar();
            sql = "INSERT INTO DATOS_DELITOS_ADOJC VALUES("+entidad+","+municipio+","+distrito+","+numero+",'" 
                    + expediente +jConcatenado + "','" + delitoClave + jConcatenado+"',"
                    + delitoCP + ","
                    + "'" + articuloCP + "',"
                    + delitoNT + ","
                    + fuero + ","
                    + reclasificaDel + ","
                    + "'" + fechaReclaDel + "',"
                    + consumacion + ","
                    + calificacion + ","
                    + concurso + ","
                    + clasificacion + ","
                    + comision + ","
                    + accion + ","
                    + modalidad + ","
                    + instrumentos + ","
                    + ocurrencia + ","
                    + entidadD + ","
                    + municipioD + ","
                    + numAdo + ","
                    + numVic + ","
                    + "'" + comentarios + "',"
                    + " (select YEAR(NOW())) );";
            System.out.println(sql);
            if (conn.escribir(sql)) {// si se inserta redirige a elementosPrincipales.jsp
                conn.close();
//                response.sendRedirect("elementosPrincipales.jsp?insertado=true");
            } else {//regresa a procesados.jsp y maca error
                conn.close();
//                response.sendRedirect("procesados.jsp?insertado=false");
            }
        } catch (Exception ex) {
            Logger.getLogger(insrtProcesados.class.getName()).log(Level.SEVERE, null, ex);
        }

        
        response.setContentType("text/html;charset=UTF-8");
        PrintWriter out = response.getWriter();
        try {
            /* TODO output your page here. You may use following sample code. */
            out.println("<!DOCTYPE html>");
            out.println("<html>");
            out.println("<head>");
            out.println("<title>Servlet insrtDelitos</title>");            
            out.println("</head>");
            out.println("<body>");
            out.println("<h1>Servlet insrtDelitos at " + request.getContextPath() + "</h1>");
            out.println("</body>");
            out.println("</html>");
        } finally {
            out.close();
        }
    }
    
    public String generaDelitoClave(String exp) throws SQLException {

        int maxDel = 0;
        String delitoClave = "";

        conn.Conectar();
        String sql = "SELECT MAX("
                                + "SUBSTR( DELITO_CLAVE, INSTR(DELITO_CLAVE,'D')+1, length(DELITO_CLAVE) )"
                            + " ) AS NUMERO"
                    + " FROM DATOS_DELITOS_ADOJC WHERE EXPEDIENTE_CLAVE='" + exp + "';";
        rs = conn.consultar(sql);
        if (rs.next()) {
            maxDel = rs.getInt("NUMERO");
        }

        int newDel = maxDel + 1;
        delitoClave = exp + "-D" + newDel;
        return delitoClave;
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