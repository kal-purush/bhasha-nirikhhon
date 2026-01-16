package controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet(name = "Login", urlPatterns = {"/Login", "/login", "/conectar"})
public class Login extends HttpServlet {

    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
                      
                    try (PrintWriter out = response.getWriter()) {
                        response.setContentType("text/html;charset=UTF-8");
                        String login = request.getParameter("login");
                        String senha = request.getParameter("senha");
                        
                        
                    //jdbc:mysql://127.0.0.1:3306/?user=root
                    if(login!=null && senha!=null){                  
                    
                    Class.forName("com.mysql.jdbc.Driver").newInstance();
                    Connection conn = DriverManager.getConnection("jdbc:mysql:localhost/rscsis");
                    String Query = "select * from usuario where login = ? and senha = ?" ;
                    PreparedStatement psm =  conn.prepareStatement(Query);
                    psm.setString(1, login);
                    psm.setString(2, senha);
                    ResultSet rs = psm.executeQuery();
                                


                    if(rs.next()){
                        
                   // response.sendRedirect("index.jsp");
                    out.println("Login Sucefull");
                    
                    }
                    
                    else{
                        
                    System.out.println("Falha no Login!");
                    
                    }   
                    
                    }
            }
            
            catch(Exception ex){
            System.out.println("Exception : "+ex.getMessage());
            }
          
      
   
    }

    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>

}