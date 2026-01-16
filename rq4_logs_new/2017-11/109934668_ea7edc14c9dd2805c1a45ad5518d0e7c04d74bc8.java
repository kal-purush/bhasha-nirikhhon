package BaseDatos;

import Clases.Asesor;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;

/**
 *
 * @author Parker
 */
public class AsesorDBHelper {
    static DBConexion conexion = null;
    
    public AsesorDBHelper(){
        conexion = new DBConexion();
    }
    
    /*
    * Se Registra un asesor en la base de datos
    */
    public boolean Registrar(Asesor asesor){
        boolean transaccion = true;
        
        try{
            Statement sentencia = conexion.getStatement();
                                    
            String query = "INSERT INTO ASESOR VALUES ((SELECT COUNT(*) FROM ASESOR) + 1, '" + asesor.getNombre() + "', '" + asesor.getApellidoPaterno() + "', '" + asesor.getApellidoMaterno() + "', '" + asesor.getContrasena() + "', '" + asesor.getCubiculo() + "', '" + asesor.getEmail() + "', TO_DATE('0001-01-01 " + asesor.getHrInicioAsesoria() + "','YYYY-MM-DD HH24:MI:SS'), TO_DATE('0001-01-01 " + asesor.getHrFinAsesoria() + "','YYYY-MM-DD HH24:MI:SS'))";            
            
            sentencia.execute(query);
            Confirmar();
        }
        catch(SQLException ex){
            System.out.println(ex);
            transaccion = false;
        }
        
        return transaccion;
    }
    
    /*
    * Se Obtiene el id de un asesor por su contraseña.
    */
    public int getIdAsesor(String contrasena){
        int id = 0;
        
        try{
            Statement sentencia = conexion.getStatement();
                                    
            String query = "SELECT IDASESOR FROM ASESOR WHERE CONTRASENA = '" + contrasena + "'";
            ResultSet resultado = sentencia.executeQuery(query);
            
            while (resultado.next()){
                id = resultado.getInt("IDASESOR");
            }            
        }
        catch(SQLException ex){
            System.out.println(ex);
        }
        
        return id;
    }
    
    /*
    * Se Obtiene un asesor por su contraseña.
    */
    public Asesor getAsesor(String contrasena){
        Asesor asesor = null;
        
        try{
            Statement sentencia = conexion.getStatement();
                                    
            String query = "SELECT * FROM ASESOR WHERE CONTRASENA = '" + contrasena + "'";
            ResultSet resultado = sentencia.executeQuery(query);
            
            while (resultado.next()){
                int idAsesor = resultado.getInt("IDASESOR");
                String nombre = resultado.getString("NOMBRE");
                String apPaterno = resultado.getString("APELLIDOPATERNO");
                String apMaterno = resultado.getString("APELLIDOMATERNO");
                String cubiculo = resultado.getString("CUBICULO");
                String email = resultado.getString("EMAIL");
//                Time hrInicioAsesoria = resultado.getDate("HRINICIOASESORIA");
//                Time hrFinAsesoria = resultado.getDate("HRFINASESORIA");
                
                asesor = new Asesor(nombre, apPaterno, apMaterno, contrasena);
                asesor.setId(idAsesor);
                asesor.setCubiculo(cubiculo);
                asesor.setEmail(email);
//                asesor.setHrInicioAsesoria(hrInicioAsesoria);
//                asesor.setHrFinAsesoria(hrFinAsesoria);
            }            
        }
        catch(SQLException ex){
            System.out.println(ex);
        }
        
        return asesor;
    }
    
    public boolean Confirmar(){
        boolean confirmacion = true;
        try{
            Statement sentencia = conexion.getStatement();
            
            String query = "COMMIT";            
            sentencia.execute(query);
        }
        catch(SQLException ex){
            System.out.println(ex);
            confirmacion = false;
        }
        return confirmacion;
    }
}