/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package daos;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 * @author guill
 */
public class Conexion {

   private Connection conexion = null;
    private String url = "jdbc:postgresql://localhost:5432/conta";
    private String usuario = "postgres";
    private String password = "root";//root

    public Connection getConexion() {
        
        try {
            Class.forName("org.postgresql.Driver").newInstance();
            this.conexion = DriverManager.getConnection(url, usuario, password);
            System.out.println("conectando a la BD");
        } catch (SQLException ex) {
        } catch (Exception e) {

        }
        return this.conexion;
    }

    public void cerrarConexiones() {
        try{
            this.conexion.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}