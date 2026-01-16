/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package clasesAuxiliar;

import ConexionDB.Conexion_Mysql;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author CARLOS.SANCHEZG
 */
public class showCausasPenales {
    Conexion_Mysql conn = new Conexion_Mysql();
    ArrayList<String[]> lista;
    String sql;
    ResultSet rs;
    
    public ArrayList findCausasPorJuzgado(String juzgado){
        try {
            conn.Conectar();
            lista = new ArrayList();
            
            sql = "SELECT E.*,TP.*, C.* "
                + " FROM DATOS_EXPEDIENTES_ADOJC E, CATALOGOS_PROCEDIMIENTO TP, CATALOGOS_RESPUESTA_SIMPLE C"
                + " WHERE TP.PROCEDIMIENTO_ID=E.TIPO_PROCEDIMIENTO"
                + " AND C.RESPUESTA_ID=E.COMPETENCIA"
                + " AND JUZGADO_CLAVE='"+juzgado+"' ORDER BY 1;";
            
            System.out.println(sql);
            rs = conn.consultar(sql);
            while (rs.next()) {
                lista.add(new String[]{
                    rs.getString("expediente_clave")
                    //rs.getString("TP.DESCRIPCION"),rs.getString("TOTAL_PROCESADOS")
                    //rs.getString("TOTAL_VICTIMAS"),rs.getString("TOTAL_DELITOS"),rs.getString("C.DESCRIPCION"),rs.getString("FECHA_INGRESO")
                });
                System.out.println(rs.getString("expediente_clave"));
            }
            conn.close();
        } catch (SQLException ex) {
            Logger.getLogger(showCausasPenales.class.getName()).log(Level.SEVERE, null, ex);
        }
        return lista;
    }
}