package BaseDatos;

import Clases.Candidato;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author Parker
 */
public class CandidatoDBHelper {
    static DBConexion conexion = null;
    
    public CandidatoDBHelper(){
        conexion = new DBConexion();
    }
    
    public boolean Registrar(Candidato candidato){
        boolean transaccion = true;
        
        try{
            String celular = "";
            Statement sentencia = conexion.getStatement();
            
            if(candidato.getCelular() == 0){
                celular = "''";
            }
            
            String query = "INSERT INTO CANDIDATO VALUES ((SELECT COUNT(*) FROM CANDIDATO) + 1, '', '', '', '" + candidato.getNombre() + "', '" + candidato.getApellidoPaterno() + "', '" + candidato.getApellidoMaterno() + "', '" + candidato.getBoleta() + "', '" + candidato.getEmail() + "', " + celular + ", '" + candidato.getCarrera() + "', " + candidato.getGeneración() + ", '" + candidato.getTemaTesis() + "', '" + candidato.getDirectorTesis() + "', '" + candidato.getLugarTrabajo() + "', '" + candidato.getHorarioTrabajo() + "', EMPTY_BLOB(), EMPTY_BLOB())";            
            
            sentencia.execute(query);
            Confirmar();
        }
        catch(SQLException ex){
            System.out.println(ex);
            transaccion = false;
        }
        
        return transaccion;
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