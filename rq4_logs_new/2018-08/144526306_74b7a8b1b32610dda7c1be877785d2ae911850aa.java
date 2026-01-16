/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Dados;

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author danie
 */
@WebServlet(name = "Servletsept", urlPatterns = {"/Servletsept"})
public class Servletsept extends HttpServlet {

    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
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
            out.println("<title>Servlet Servletsept</title>");    
            out.println("<link rel='stylesheet' type='text/css' href='" + request.getContextPath() +  "/newcss.css' />");
            out.println("</head>");
            out.println("<body>");
            out.println("<h1>Setor De Educação Profissional e Tecnologica</h1>");
            out.println("<h2>R. Dr. Alcides Vieira Arcoverde, 1225 - Jardim das Americas, Curitiba - PR, 81520-260</h>");
            out.println("<table>");
            out.println ("<tr>");
                out.println("<th>Course</th>");
            out.println("<th>Contact</th>");
            out.println("</tr>");
            out.println("<tr>");
            out.println("<td>TADS</td>");
            out.println("<td><a href= 'http://www.sept.ufpr.br/portal/analisesistemas/'>Clique Aqui</a></td>");
            out.println("</tr>");
            out.println("  <tr>\n" +
            "    <td>Agente Comunitário da Saúde</td>\n" +
               "    <td><a href= 'http://200.17.200.17/agentecomunitario.html'>Clique Aqui</a></td>\n" +
"  </tr>");
            out.println("<tr>\n" +
"    <td>Petróleo e Gás</td>\n" +
"    <td><a href= '//www.sept.ufpr.br/portal/petroleogas/'>Clique Aqui</a></td>\n" +
"  </tr>");
            
            out.println("<tr>\n" +
"    <td>Comunicação Institucional</td>\n" +
"    <td><a href= 'http://www.sept.ufpr.br/portal/comunicacaoinstitucional/'>Clique Aqui</a></td>\n" +
"  </tr>");
           out.println("<tr>\n" +
"    <td>Gestão da Qualidade</td>\n" +
"    <td><a href= 'http://www.sept.ufpr.br/portal/gestaoqualidade/'>Clique Aqui</a></td>\n" +
"  </tr>");
           out.println("<tr>\n" +
"    <td>Gestão Pública</td>\n" +
"    <td><a href= 'http://www.sept.ufpr.br/portal/gestaopublica/'>Clique Aqui</a></td>\n" + 
"  </tr>");
           out.println("<tr>\n" +
"    <td>Luteria</td>\n" +
"    <td><a href= 'http://www.sept.ufpr.br/portal/luteria/'>Clique Aqui</a></td>\n" +
"  </tr>");
           out.println("<tr>\n" +
"    <td>Negócios Imobiliários</td>\n" +
"    <td><a href= 'http://200.17.200.17/negociosimobiliarios.html'>Clique Aqui</a></td>\n" +
"  </tr>");
           out.println("<tr>\n" +
"    <td>Produção Cenicas</td>\n" +
"    <td><a href= 'http://www.tpc.ufpr.br/'>Clique Aqui</a></td>\n" +
"  </tr>");
           
            out.println("<tr>\n" +
"    <td>Secretariado</td>\n" +
"    <td><a href= 'http://www.sept.ufpr.br/portal/secretariado/'>Clique Aqui</a></td>\n" +
"  </tr>");
            out.println("<tr>\n" +
"    <td>Programa de Pós-Graduação em Bioinformática</td>\n" +
"    <td><a href= 'http://www.bioinfo.ufpr.br/'>Clique Aqui</a></td>\n" +  
"  </tr>");
            out.println("</table>");
            
         out.println("<a href= 'http://localhost:8084/SeptServe/Meutads.jsp'>Meu Sept</a>");
            
            out.println("</body>");
            out.println("</html>");
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