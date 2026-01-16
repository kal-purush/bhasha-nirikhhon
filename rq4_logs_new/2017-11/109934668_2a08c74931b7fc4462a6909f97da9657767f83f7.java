package BaseDatos;
import java.sql.*;
/**
 *
 * @author Parker
 */
public class DBConexion {
    static String url;
    static String usuario;
    static String contrasena;
    static Connection conexion = null;
    static boolean estado = true;

    public DBConexion() {
        DBConexion.url = "";
        
        try{
            DBConexion.usuario = "";
            DBConexion.contrasena = "";
            conexion = DriverManager.getConnection(url, DBConexion.usuario, DBConexion.contrasena);            
        }
        catch(Exception e){
            estado = false;
        }        
    }
    
    public DBConexion(String usuario, String contrasena) {
        DBConexion.url = "";
        
        try{
            DBConexion.usuario = usuario;
            DBConexion.contrasena = contrasena;
            conexion = DriverManager.getConnection(url, DBConexion.usuario, DBConexion.contrasena);            
        }
        catch(Exception e){
            estado = false;
        }        
    }
    
    public void closeConexion(){
        try{
            if(conexion != null && !conexion.isClosed())
                conexion.close();
        }
        catch(Exception e){
            estado = false;
        }
    }
    
    public Statement getStatement(){
        try{
            if (conexion.isClosed())
                return null;
            
            return conexion.createStatement();
        }
        catch(Exception e){
            return null;
        }        
    }
}