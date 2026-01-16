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
public class showJueces {
    Conexion_Mysql conn = new Conexion_Mysql();
    ArrayList<String> lista;
    ArrayList<String[]> listaTabla;
    String sql;
    ResultSet rs;
    
    public ArrayList findJuez(){
        try {
            conn.Conectar();
            lista = new ArrayList();
            sql = "SELECT JUEZ_CLAVE FROM DATOS_JUECES_ADOJC ORDER BY 1;";
            rs = conn.consultar(sql);
            while (rs.next()) {
                lista.add(rs.getString(1));
            }
            conn.close();
        } catch (SQLException ex) {
            Logger.getLogger(showJueces.class.getName()).log(Level.SEVERE, null, ex);
        }
        return lista;
    }
    
    public ArrayList findjuezTabla(String juzClave){
        try {
            conn.Conectar();
            listaTabla = new ArrayList<String[]>();
            sql = "SELECT DJ.JUEZ_CLAVE,CONCAT(DJ.NOMBRE_JUEZ,' ',DJ.APELLIDOP_JUEZ,' ',DJ.APELLIDOM_JUEZ) AS NOMBRE_JUEZ,DJ.EDAD,DJ.FECHA_GESTION,"
                    + "CF.DESCRIPCION "
                    + "FROM DATOS_JUECES_ADOJC DJ "
                    + "JOIN CATALOGOS_FUNCION_JUZGADO CF "
                    + "ON DJ.FUNCION_DESEMPENA = CF.FUNCION_JUZ_ID "
                    + "WHERE JUZGADO_CLAVE = '" + juzClave + "' "
                    + "ORDER BY 1";
            rs = conn.consultar(sql);
            while (rs.next()) {
                listaTabla.add(new String[]{
                    rs.getString(1),rs.getString(2),rs.getString(3),rs.getString(4),rs.getString(5)
                });
            }
            conn.close();
        } catch (SQLException ex) {
            Logger.getLogger(showJueces.class.getName()).log(Level.SEVERE, null, ex);
        }
        return listaTabla;
    }
    
    public int findMaxJuex(String juzClave){
        int max = 0;
        try {
            conn.Conectar();
            sql = "SELECT MAX(JUEZ_CLAVE) "
                    + "FROM DATOS_JUECES_ADOJC "
                    + "WHERE JUZGADO_CLAVE = '" + juzClave + "' "
                    + "ORDER BY JUEZ_CLAVE";
            rs = conn.consultar(sql);
            while (rs.next()) {
                max = rs.getInt(1);
            }
            conn.close();
        } catch (SQLException ex) {
            Logger.getLogger(showJueces.class.getName()).log(Level.SEVERE, null, ex);
        }
        return max;
    }
    
}