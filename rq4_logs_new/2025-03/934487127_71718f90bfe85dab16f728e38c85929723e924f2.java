/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */






package Modelo.seguridad;


import Controlador.seguridad.RelPerfApl;
import Modelo.Conexion;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author visitante
 */
public class RelPerfAplDAO {

    private static final String SQL_SELECT = "SELECT aplicacion_codigo, perfil_codigo, consultar_rpa,actualizar_rpa,eliminar_rpa,imprimir_rpa,insertar_rpa FROM relperfapl";
    private static final String SQL_INSERT = "INSERT INTO relperfapl(aplicacion_codigo, perfil_codigo,consultar_rpa,actualizar_rpa,eliminar_rpa,imprimir_rpa,insertar_rpa) VALUES(?,?,?,?,?,?,?)";
    private static final String SQL_UPDATE = "UPDATE relperfapl SET consultar_rpa=?,actualizar_rpa=?,eliminar_rpa=?,imprimir_rpa=?,insertar_rpa=? WHERE aplicacion_codigo=?,perfil_codigo=?";
    private static final String SQL_DELETE = "DELETE FROM relperfapl WHERE aplicacion_codigo=?,perfil_codigo=?";
    private static final String SQL_QUERY = "SELECT aplicacion_codigo, perfil_codigo, consultar_rpa,actualizar_rpa,eliminar_rpa,imprimir_rpa,insertar_rpa FROM relperfapl WHERE aplicacion_codigo= ?,perfil_codigo=?";

    public List<RelPerfApl> select() {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        RelPerfApl relPerfApl = null;
        List<RelPerfApl> list_relPerfApl = new ArrayList<RelPerfApl>();

        try {
            conn = Conexion.getConnection();
            stmt = conn.prepareStatement(SQL_SELECT);
            rs = stmt.executeQuery();
            while (rs.next()) {
                int aplicacion_codigo = rs.getInt("aplicacion_codigo");
                int perfil_codigo = rs.getInt("perfil_codigo");
                String consultar_rpa = rs.getString("consultar_rpa");
                String actualizar_rpa = rs.getString("actualizar_rpa");
                String eliminar_rpa = rs.getString("eliminar_rpa");
                String imprimir_rpa = rs.getString("imprimir_rpa");
                String insertar_rpa = rs.getString("insertar_rpa");
                
                relPerfApl = new RelPerfApl();
                relPerfApl.setAplicacion_codigo(aplicacion_codigo);
                relPerfApl.setPerfil_codigo(perfil_codigo);
                relPerfApl.setConsultar_rpa(consultar_rpa);
                relPerfApl.setActualizar_rpa(actualizar_rpa);
                relPerfApl.setEliminar_rpa(eliminar_rpa);
                relPerfApl.setImprimir_rpa(imprimir_rpa);
                relPerfApl.setInsertar_rpa(insertar_rpa);
              
                
                list_relPerfApl.add(relPerfApl);
              
            }

        } catch (SQLException ex) {
            ex.printStackTrace(System.out);
        } finally {
            Conexion.close(rs);
            Conexion.close(stmt);
            Conexion.close(conn);
        }

        return list_relPerfApl;
    }

    public int insert(RelPerfApl relPerfApl) {
        Connection conn = null;
        PreparedStatement stmt = null;
        int rows = 0;
        try {
            conn = Conexion.getConnection();
            stmt = conn.prepareStatement(SQL_INSERT);
            stmt.setInt(1, relPerfApl.getAplicacion_codigo());
            stmt.setInt(2, relPerfApl.getPerfil_codigo());
            stmt.setString(3, relPerfApl.getConsultar_rpa());
            stmt.setString(4, relPerfApl.getActualizar_rpa());
            stmt.setString(5, relPerfApl.getEliminar_rpa());
            stmt.setString(6, relPerfApl.getImprimir_rpa());
            stmt.setString(7, relPerfApl.getInsertar_rpa());
         


            System.out.println("ejecutando query:" + SQL_INSERT);
            rows = stmt.executeUpdate();
            System.out.println("Registros afectados:" + rows);
        } catch (SQLException ex) {
            ex.printStackTrace(System.out);
        } finally {
            Conexion.close(stmt);
            Conexion.close(conn);
        }

        return rows;
    }

    public int update(RelPerfApl relPerfApl) {
        Connection conn = null;
        PreparedStatement stmt = null;
        int rows = 0;

        try {
            conn = Conexion.getConnection();
            System.out.println("ejecutando query: " + SQL_UPDATE);
            stmt = conn.prepareStatement(SQL_UPDATE);
            stmt.setString(1, relPerfApl.getConsultar_rpa());
            stmt.setString(2, relPerfApl.getActualizar_rpa());
            stmt.setString(3, relPerfApl.getEliminar_rpa());
            stmt.setString(4, relPerfApl.getImprimir_rpa());
            stmt.setString(5, relPerfApl.getInsertar_rpa());
         
            //comodin del where
            stmt.setInt(6,relPerfApl.getAplicacion_codigo());
            stmt.setInt(7,relPerfApl.getPerfil_codigo());
           

            rows = stmt.executeUpdate();
            System.out.println("Registros actualizado:" + rows);

        } catch (SQLException ex) {
            ex.printStackTrace(System.out);
        } finally {
            Conexion.close(stmt);
            Conexion.close(conn);
        }

        return rows;
    }

    public int delete(RelPerfApl relPerfApl) {
        Connection conn = null;
        PreparedStatement stmt = null;
        int rows = 0;

        try {
            conn = Conexion.getConnection();
            System.out.println("Ejecutando query:" + SQL_DELETE);
            stmt = conn.prepareStatement(SQL_DELETE);
            stmt.setInt(1, relPerfApl.getAplicacion_codigo());
            stmt.setInt(2, relPerfApl.getPerfil_codigo());
            rows = stmt.executeUpdate();
            System.out.println("Registros eliminados:" + rows);
        } catch (SQLException ex) {
            ex.printStackTrace(System.out);
        } finally {
            Conexion.close(stmt);
            Conexion.close(conn);
        }

        return rows;
    }

//    public List<Persona> query(Persona vendedor) { // Si se utiliza un ArrayList
    public RelPerfApl query(RelPerfApl relPerfApl) {    
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        List<RelPerfApl> list_relPerfApl = new ArrayList<RelPerfApl>();
        int rows = 0;

        try {
            conn = Conexion.getConnection();
            System.out.println("Ejecutando query:" + SQL_QUERY);
            stmt = conn.prepareStatement(SQL_QUERY);
            stmt.setInt(1, relPerfApl.getAplicacion_codigo());
            stmt.setInt(2, relPerfApl.getPerfil_codigo());
            rs = stmt.executeQuery();
            while (rs.next()) {
             int aplicacion_codigo = rs.getInt("aplicacion_codigo");
                int perfil_codigo = rs.getInt("perfil_codigo");
                String consultar_rpa = rs.getString("consultar_rpa");
                String actualizar_rpa = rs.getString("actualizar_rpa");
                String eliminar_rpa = rs.getString("eliminar_rpa");
                String imprimir_rpa = rs.getString("imprimir_rpa");
                String insertar_rpa = rs.getString("insertar_rpa");
                
                relPerfApl = new RelPerfApl();
                relPerfApl.setAplicacion_codigo(aplicacion_codigo);
                relPerfApl.setPerfil_codigo(perfil_codigo);
                relPerfApl.setConsultar_rpa(consultar_rpa);
                relPerfApl.setActualizar_rpa(actualizar_rpa);
                relPerfApl.setEliminar_rpa(eliminar_rpa);
                relPerfApl.setImprimir_rpa(imprimir_rpa);
                relPerfApl.setInsertar_rpa(insertar_rpa);
              
                
               
                
                
            }
            //System.out.println("Registros buscado:" + vendedor);
        } catch (SQLException ex) {
            ex.printStackTrace(System.out);
        } finally {
            Conexion.close(rs);
            Conexion.close(stmt);
            Conexion.close(conn);
        }

       
        return relPerfApl;
    }
            
}