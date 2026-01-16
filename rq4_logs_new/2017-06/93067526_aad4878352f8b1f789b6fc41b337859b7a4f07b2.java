/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sqlitebd;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 *
 * @author kevin
 */
public class BaseDatos {

    private String url;
    private Connection conn = null;

    public BaseDatos() {
    }

    public BaseDatos(String url) {
        this.url = "jdbc:sqlite:" + url;
    }
/**
 * Metodo para conectarse a la base de datos con la url dada en el constructor
 */
    public void connect() {
        try {
            conn = DriverManager.getConnection(url);
            if (conn != null) {
                System.out.println("Conectado");
            }
        } catch (SQLException ex) {
            System.err.println("No se ha podido conectar a la base de datos");
        }
    }
/**
 * Recibe los parametros que vamos a insertar en nuestra base de datos, devuelve el numero de insercciones correctas
 * @param id
 * @param nombre
 * @param precio
 * @return 
 */
    public int insertar(String id, String nombre, float precio) {
        int cont = 0;
        try {
            PreparedStatement st = conn.prepareStatement("insert into productos (id,nombre, precio) values (?,?,?)");
            st.setString(1, id);
            st.setString(2, nombre);
            st.setFloat(3, precio);
            st.execute();
            cont += 1;
            return cont;
        } catch (SQLException ex) {
            return cont;
        }
    }
/**
 * Recibe un id del producto que vamos a actualizar y el nuevo valor por cada parametro en nuestra base de datos, devuelve el numero de actualizaciones correctas
 * @param id
 * @param precio
 * @param nombreNuevo
 * @return 
 */
    public int update(String id, float precio, String nombreNuevo) {
        int cont = 0;
        try {
            String sql = "UPDATE productos SET nombre = ? , "
                    + "precio = ? "
                    + "WHERE id = ?";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, nombreNuevo);
            pstmt.setFloat(2, precio);
            pstmt.setString(3, id);
            pstmt.executeUpdate();
            cont += 1;
            return cont;
        } catch (SQLException ex) {
            return cont;
        }
    }
/**
 * Recibe el id del producto a borrar en nuestra base de datos, devuelve el numero de eliminaciones correctas
 * @param id
 * @return 
 */
    public int borrar(String id) {
        int cont = 0;
        try {
            String sql = "DELETE FROM productos WHERE id = ?";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, id);
            pstmt.executeUpdate();
            cont += 1;
            return cont;
        } catch (SQLException ex) {
            return cont;
        }
    }
}