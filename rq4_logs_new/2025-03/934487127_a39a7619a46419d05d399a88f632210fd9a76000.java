/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Modelo.seguridad;

import Controlador.Perfil; 
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
public class PerfilDAO {

    private static final String SQL_SELECT = "SELECT id_perfil, nombre_perfil, estatus_perfil FROM perfiles";
    private static final String SQL_INSERT = "INSERT INTO perfiles(nombre_perfil, estatus_perfil) VALUES(?, ?)";
    private static final String SQL_UPDATE = "UPDATE perfiles SET nombre_perfil=?, estatus_perfil=? WHERE id_perfil = ?";
    private static final String SQL_DELETE = "DELETE FROM perfiles WHERE id_perfil=?";
    private static final String SQL_QUERY = "SELECT id_perfil, nombre_perfil, estatus_perfil FROM perfiles WHERE id_perfil = ?";

    public List<Perfil> select() {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        Perfil perfil = null;
        List<Perfil> perfiles = new ArrayList<Perfil>();

        try {
            conn = Conexion.getConnection();
            stmt = conn.prepareStatement(SQL_SELECT);
            rs = stmt.executeQuery();
            while (rs.next()) {
                int idPerfil = rs.getInt("id_perfil");
                String nombrePerfil = rs.getString("nombre_perfil");
                String estatusPerfil = rs.getString("estatus_perfil");
                
                perfil = new Perfil();
                perfil.setId_perfil(idPerfil);
                perfil.setNombre_perfil(nombrePerfil);
                perfil.setEstatus_perfil(estatusPerfil);
                
                perfiles.add(perfil);
            }

        } catch (SQLException ex) {
            ex.printStackTrace(System.out);
        } finally {
            Conexion.close(rs);
            Conexion.close(stmt);
            Conexion.close(conn);
        }

        return perfiles;
    }

    public int insert(Perfil perfil) { 
        Connection conn = null;
        PreparedStatement stmt = null;
        int rows = 0;
        try {
            conn = Conexion.getConnection();
            stmt = conn.prepareStatement(SQL_INSERT);
            stmt.setString(1, perfil.getNombre_perfil());
            stmt.setString(2, perfil.getEstatus_perfil());

            System.out.println("ejecutando query: " + SQL_INSERT);
            rows = stmt.executeUpdate();
            System.out.println("Registros afectados: " + rows);
        } catch (SQLException ex) {
            ex.printStackTrace(System.out);
        } finally {
            Conexion.close(stmt);
            Conexion.close(conn);
        }

        return rows;
    }

    public int update(Perfil perfil) {
        Connection conn = null;
        PreparedStatement stmt = null;
        int rows = 0;

        try {
            conn = Conexion.getConnection();
            System.out.println("ejecutando query: " + SQL_UPDATE);
            stmt = conn.prepareStatement(SQL_UPDATE);
            
            stmt.setString(1, perfil.getNombre_perfil());
            stmt.setString(2, perfil.getEstatus_perfil());
            stmt.setInt(3, perfil.getId_perfil());
            
            rows = stmt.executeUpdate();
            System.out.println("Registros actualizado: " + rows);

        } catch (SQLException ex) {
            ex.printStackTrace(System.out);
        } finally {
            Conexion.close(stmt);
            Conexion.close(conn);
        }

        return rows;
    }

    public int delete(Perfil perfil) {
        Connection conn = null;
        PreparedStatement stmt = null;
        int rows = 0;

        try {
            conn = Conexion.getConnection();
            System.out.println("Ejecutando query: " + SQL_DELETE);
            stmt = conn.prepareStatement(SQL_DELETE);
            stmt.setInt(1, perfil.getId_perfil());
            rows = stmt.executeUpdate();
            System.out.println("Registros eliminados: " + rows);
        } catch (SQLException ex) {
            ex.printStackTrace(System.out);
        } finally {
            Conexion.close(stmt);
            Conexion.close(conn);
        }

        return rows;
    }

    public Perfil query(Perfil perfil) {    
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        List<Perfil> perfiles = new ArrayList<Perfil>();
        int rows = 0;

        try {
            conn = Conexion.getConnection();
            System.out.println("Ejecutando query: " + SQL_QUERY);
            stmt = conn.prepareStatement(SQL_QUERY);
            stmt.setInt(1, perfil.getId_perfil());
            rs = stmt.executeQuery();
            while (rs.next()) {
                int idPerfil = rs.getInt("id_perfil");
                String nombrePerfil = rs.getString("nombre_perfil");
                String estatusPerfil = rs.getString("estatus_perfil");
                
                perfil = new Perfil();
                perfil.setId_perfil(idPerfil);
                perfil.setNombre_perfil(nombrePerfil);
                perfil.setEstatus_perfil(estatusPerfil);

            }

        } catch (SQLException ex) {
            ex.printStackTrace(System.out);
        } finally {
            Conexion.close(rs);
            Conexion.close(stmt);
            Conexion.close(conn);
        }

        return perfil; 
    }
        
}