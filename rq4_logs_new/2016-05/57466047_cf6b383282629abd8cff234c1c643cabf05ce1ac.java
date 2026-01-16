/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package basededatos;
import java.sql.*;

/**
 *
 * @author digut
 */
public class conexion {
   private Connection con;
   private void conectar()
   {
       con = null;
       try
       {
           Class.forName("com.mysql.jdbc.Driver").newInstance();
           System.out.println("Registro exitoso");
       }
       catch(Exception e)
       {
           System.out.println(e.toString());
       }
       
       try{
           con = DriverManager.getConnection("jdbc:mysql://localhost/next.book?user=root&password=");
       } catch (SQLException ex){
           System.out.println("SQLException: "+ ex.getMessage());
       }
   }
   
   public ResultSet consulta(String query)
   {
       con = null;
       ResultSet rs = null;
       Statement cmd = null;
       
       conectar();
       
       try{
           cmd = con.createStatement();
           
           rs = cmd.executeQuery(query);
           /*
            while (rs.next()) {
                 String nombre = rs.getString("nombre") + " " + rs.getString("apellido") ;
                 System.out.println(nombre);
             }

             rs.close();*/
       }
       catch (SQLException ex)
       {
           System.out.println("SQLException: "+ ex.getMessage());
       }
       
       return rs;
   }
}
