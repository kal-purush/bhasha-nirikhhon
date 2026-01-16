package BaseDatos;

import static BaseDatos.AsesorDBHelper.conexion;
import Clases.Grupo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author Parker
 */
public class GrupoDBHelper {
    static DBConexion conexion = null;
    
    public GrupoDBHelper(){
        conexion = new DBConexion();
    }
    
    /*
    * Se Registra un grupo en la base de datos
    */
    public boolean Registrar(Grupo grupo){
        boolean transaccion = true;
        
        try{
            Statement sentencia = conexion.getStatement();
                                    
            String query = "INSERT INTO GRUPO VALUES ((SELECT COUNT(*) FROM GRUPO) + 1, '" + grupo.getNombre() + "')";            
            
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
    * Se Obtiene el numero de candidatos que tiene el último grupo registrado, la busqueda se hace por id.
    */
    public int getNumCandidatos(Grupo grupo){
        int numCandidatos = 0;
        
        try{
            Statement sentencia = conexion.getStatement();
                                    
            String query = "SELECT COUNT(IDGRUPO) AS IDGRUPO FROM CANDIDATO WHERE IDGRUPO = " + grupo.getId();
            ResultSet resultado = sentencia.executeQuery(query);
            
            while (resultado.next()){
                numCandidatos = resultado.getInt("IDGRUPO");
            }            
        }
        catch(SQLException ex){
            System.out.println(ex);
        }
        
        return numCandidatos;
    }
    
    /*
    * Se Obtiene el último grupo registrado en la base de datos.
    */
    public Grupo getUltimoGrupo(){
        Grupo grupo = null;
        
        try{
            Statement sentencia = conexion.getStatement();
                                    
            String query = "SELECT * FROM (SELECT ROWNUM AS RENGLON, IDGRUPO, NOMBRE FROM GRUPO) WHERE RENGLON = (SELECT MAX(ROWNUM) FROM GRUPO)";
            ResultSet resultado = sentencia.executeQuery(query);
            
            while (resultado.next()){
                int idGrupo = resultado.getInt("IDGRUPO");
                String Nombre = resultado.getString("NOMBRE");
                
                grupo = new Grupo(Nombre);
                grupo.setId(idGrupo);
            }            
        }
        catch(SQLException ex){
            System.out.println(ex);
        }
        
        return grupo;
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