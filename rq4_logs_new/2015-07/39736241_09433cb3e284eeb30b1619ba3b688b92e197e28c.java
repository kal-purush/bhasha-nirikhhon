/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import java.io.IOException;

/**
 *
 * @author diana and paula
 */
@WebServlet(urlPatterns = {"/StartServlet"})
public class StartServlet extends HttpServlet {

    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
       static AmazonDynamoDBClient dynamoDBClient;
        public static String TABLENAME = "finalTables";
        private static final String bucketName     = "pdsphinxbucket";
        private static final String dbName = "mydb";
        private static final String userName = "pmontojo";
        private static final String password = "pmontojo";
        private static final String hostname = "mydb.cued7orr1q2t.us-west-2.rds.amazonaws.com";
        private static final String port = "3306";
        private static final String jdbcUrl = "jdbc:mysql://" + hostname + ":" +
          port + "/" + dbName + "?user=" + userName + "&password=" + password;
        private static String[]hola;
        
            protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
            response.setContentType("text/html;charset=UTF-8");
            

            String courseName=request.getParameter("param");

            System.out.println("My course name:" + courseName);

            Connection conn = null;
            Statement setupStatement;
            Statement readStatement = null;
            ResultSet resultSet = null;
            String results = "";
            int numresults = 0;
            String statement = null;
            List<Video> result = new ArrayList<>();
            
            try{

              //Create connection to RDS instance
              conn = DriverManager.getConnection(jdbcUrl);
              setupStatement = conn.createStatement();
              String retrieveVideoName = "select * from metadata where course_name ='"+courseName+"';";
              ResultSet rs =  setupStatement.executeQuery(retrieveVideoName);

              
              while(rs.next()){
           		        Video videos = new Video();
				videos.setUrl(rs.getString("url"));
				videos.setVideoName(rs.getString("video_name"));
				videos.setCourseName(rs.getString("course_name"));
                                videos.setId(Integer.parseInt(rs.getString("id")));
				result.add(videos);
                                
               System.out.println(videos.getUrl());
                
              }
              
           
           try (PrintWriter out = response.getWriter()) {
            /* TODO output your page here. You may use following sample code. */
            out.println("<!DOCTYPE html>");
            out.println("<html>");
            out.println("<head>");
            out.println("<title>Servlet Mp4Servlet</title>");
            out.println("</head>");
            out.println("<body>");
            out.println("<h1>List of videos for ITM"+courseName+"</h1>");
            
                out.println("<div>");
                for(Video video : result) {
                    String videoname =video.getVideoName() ;
                    String videourl = video.getUrl();
                    int videoId = video.getId();

                    out.println("<a href='http://localhost:8080/mystudentUI/VideoServlet?action="+videourl+"'>" +videoname+ "</a><br>");

                }
                
           
            out.println("</div><br>");
            out.println("<a href='http://localhost:8080/studentUI/myuiservlet'>BACK TO LIST </a>"); 
            out.println("</body>");
            out.println("</html>");
        }
              
            } catch (SQLException ex) {
              // handle any errors
              System.out.println("SQLException: " + ex.getMessage());
              System.out.println("SQLState: " + ex.getSQLState());
              System.out.println("VendorError: " + ex.getErrorCode());
            } finally {
              System.out.println("Closing the connection.");
              if (conn != null) try { conn.close(); } catch (SQLException ignore) {}
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