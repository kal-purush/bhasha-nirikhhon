/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Control_BD;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Provee de todos los metodos necesarios para extraer datos de MYSQL.
 * @author Zush18
 */
public class Conector {
    //<editor-fold desc="Credenciales">
    /**
     * Usuario de la base de datos
     */
    String username = "root";
    /**
     * Contrasaña para conectar
     */
    String password = "n0m3l0";
    /**
     * Ruta de la base de datos
     */
    String url = "jdbc:mysql:3306//localhost/proyecto";
    /**
     * Driver de MYSQL para conectar
     */
    String driver = "com.mysql.jdbc.Driver";
    //</editor-fold>

    //<editor-fold desc="Metodos de conexion">
    /**
     * Imprime el error en consola
     * @param e 
     */
    private void printErr(Exception e){
        System.out.println("E R R O R :"+e.getMessage());
        StackTraceElement[] errores = e.getStackTrace();
        for(StackTraceElement err : errores){
            System.out.println(err.getMethodName()+"  :  "+err.getLineNumber() +" < F I L E > "+err.getFileName());
        }
        System.out.println("E R R O R :"+e.getMessage());
    }
    /**
     * Se conecta a la base de datos de MYSQL
     * @return Connection Objeto de conexion MYSQL 
     */
    private Connection getConnection(){
        
        Connection con=null;
        
        try{
            Class.forName(driver);
            url="jdbc:mysql://localhost/proyecto";
            con=DriverManager.getConnection(url, username, password);
            
            System.out.println("Si se conecto a la BD");
        }catch(Exception e){
            this.printErr(e);
        }
        return con;
    }
    /**
     * Hace una consulta a la base de datos atravez de una sentencia SQL
     * @param sql
     * @return ResultSet Resultado
     */
    private ResultSet Query(String sql){
        throw new UnsupportedOperationException("Sin base de datos para poder programar este codigo"); 
    }
    /**
     * Actuliza  la base de datos atrvez de una base de datos a travez de una
     * sentencia SQL
     * @param sql
     * @return 
     */
    private int Update(String sql){
        throw new UnsupportedOperationException("Sin base de datos para poder programar este codigo"); 
    }
    //</editor-fold>
    
    
}