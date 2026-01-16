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
 * @author CESAR.OSORIO
 */
public class showProcesados {
    Conexion_Mysql conn = new Conexion_Mysql();
    ArrayList<String[]> proce;
    String sql;
    ResultSet resul;
    
    public ArrayList findProcesados(){
        conn.Conectar();
        proce = new ArrayList();
        sql = "SELECT * FROM DATOS_PROCESADOS_ADOJC";
        resul = conn.consultar(sql);
        try {
            while(resul.next()){
                proce.add(new String[]{resul.getString(1), resul.getString(2),resul.getString(3),
                    resul.getString(4),resul.getString(5),resul.getString(6),resul.getString(7),
                    resul.getString(8),resul.getString(9),resul.getString(10),resul.getString(11),
                    resul.getString(12),resul.getString(13),resul.getString(14),resul.getString(15),
                    resul.getString(16),resul.getString(17),resul.getString(18),resul.getString(19),
                    resul.getString(20),resul.getString(21),resul.getString(22),resul.getString(23),
                    resul.getString(24),resul.getString(25),resul.getString(26),resul.getString(27),
                    resul.getString(28),resul.getString(29),resul.getString(30),resul.getString(31),
                    resul.getString(32),resul.getString(33),resul.getString(34),resul.getString(35),
                    resul.getString(36),resul.getString(37),resul.getString(38),resul.getString(39),
                    resul.getString(40),resul.getString(41),resul.getString(42),resul.getString(43),
                    resul.getString(44),resul.getString(45),resul.getString(46),resul.getString(47)});
            }
            conn.close();
        }
        catch (SQLException ex) {
            Logger.getLogger(showTramite.class.getName()).log(Level.SEVERE, null, ex);
        }
        return proce;
    } 
    
    public ArrayList findProcesasdosTabla(){
        conn.Conectar();
        proce = new ArrayList();
        sql = "SELECT P.PROCESADO_CLAVE, CTC.DESCRIPCION, CS.DESCRIPCION, P.FECHA_NACIMIENTO, CONCAT(CM.DESCRIPCION,', ',CE.DESCRIPCION)"
                + "  FROM DATOS_PROCESADOS_ADOJC P, CATALOGOS_TIPO_CONSIGNACION CTC, CATALOGOS_SEXO CS, CATALOGOS_MUNICIPIOS CM, CATALOGOS_ENTIDADES CE WHERE "
                + " P.TIPO_CONSIGNACION = CTC.CONSIGNACION_ID AND P.SEXO = CS.SEXO_ID AND P.NACIMIENTO_MUNICIPIO = CM.MUNICIPIO_ID AND P.NACIMIENTO_ENTIDAD = CE.ENTIDAD_ID";
        resul = conn.consultar(sql);
        try {
            while(resul.next()){
                proce.add(new String[]{resul.getString(1), resul.getString(2),resul.getString(3),
                    resul.getString(4),resul.getString(5)});
            }
            conn.close();
        }
        catch (SQLException ex) {
            Logger.getLogger(showTramite.class.getName()).log(Level.SEVERE, null, ex);
        }
        return proce;
    } 
    
}