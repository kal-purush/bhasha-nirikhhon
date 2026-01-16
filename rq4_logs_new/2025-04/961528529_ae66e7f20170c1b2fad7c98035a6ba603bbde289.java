/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package pe.edu.pucp.gdptalento.core.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import pe.edu.pucp.gdptalento.core.dao.RolDAO;
import pe.edu.pucp.gdptalento.core.model.NombreRol;
import pe.edu.pucp.gdptalento.core.model.Permiso;
import pe.edu.pucp.gdptalento.core.model.Rol;
import pucp.edu.pe.gdptalento.config.DBManager;

/**
 *
 * @author raulm
 */
public class RolMySQL implements RolDAO{
    
    private Statement st;//no estoy usandolo pq estoy haciendo con preparedStatement
    private Connection con;
    private ResultSet rs;
    private PreparedStatement pst;
    
    public int insertar(Rol rol){
        int resultado = 0;
        try{
            DBManager db = new DBManager();
            con = db.getConnection();
            //Ejecuciones SQL
            String sql = "INSERT INTO Rol(nombre) VALUES(?)";
            pst = con.prepareStatement(sql);
            //st = con.createStatement();
            pst.setString(1, String.valueOf(rol.getNombre()));
            resultado=pst.executeUpdate();
            System.out.println("Se ingreso un Rol");
            ArrayList<Permiso> list_permisos= new ArrayList<Permiso>(rol.getPermisos());
            for(Permiso p : list_permisos){
                sql = "INSERT INTO Permiso (nombre) VALUES(?)";
                pst = con.prepareStatement(sql);
                pst.setString(1, String.valueOf(p));
                resultado=pst.executeUpdate();
                System.out.println("Se ingreso un permiso de rol");
            }
        }catch(Exception ex){
            System.out.println(ex.getMessage());
        }
        finally{
            try{con.close();} catch(Exception ex){System.out.println(ex.getMessage());}
        }
        return resultado;
    }
    
    public int modificar(Rol rol, int id){
        int resultado=0;
        try{
            DBManager db = new DBManager();
            con = db.getConnection();
            //Ejecuciones SQL MiembroPUCP
            String sql = "UPDATE Rol SET nombre = ? WHERE id_rol = ?";
            pst = con.prepareStatement(sql);
            pst.setString(1, String.valueOf(rol.getNombre()));
            pst.setInt(2, id);
            resultado=pst.executeUpdate();
            System.out.println("Se modifico un rol");
            //Ejecuciones SQL
            
        }catch(Exception ex){
            System.out.println(ex.getMessage());
        }finally{
            try{con.close();} catch(Exception ex){System.out.println(ex.getMessage());}
        }
        return resultado;
    }
    
    public int eliminar(int id_rol){
        int resultado=0;
        try{
            DBManager db = new DBManager();
            con = db.getConnection();
            //Ejecuciones SQL
            String sql = "DELETE FROM Rol WHERE id_rol = ?";
            pst = con.prepareStatement(sql);
            pst.setInt(1, id_rol);
            resultado=pst.executeUpdate(sql);
            System.out.println("Se elimino un rol");
            con = db.getConnection();
            //Ejecuciones SQL
            sql = "DELETE FROM Rol_Permiso WHERE id_rol = ?";
            pst = con.prepareStatement(sql);
            pst.setInt(1, id_rol);
            resultado=st.executeUpdate(sql);
            System.out.println("Se elimino un miembroPUCP");
        }catch(Exception ex){
            System.out.println(ex.getMessage());
        }finally{
            try{con.close();} catch(Exception ex){System.out.println(ex.getMessage());}
        }
        return resultado;
    }
    
    public ArrayList<Rol> listarTodas(){
        ArrayList<Rol> listadoRoles = new ArrayList<Rol>();
        try{
            DBManager db = new DBManager();
            con = db.getConnection();
            String sql = "SELECT r.nombre AS nombre_r, p.nombre AS nombre_p FROM Rol r INNER JOIN Rol_Permiso rp ON r.id_rol = rp.id_rol INNER JOIN Permiso p ON rp.id_permiso=p.id_permiso";
            pst = con.prepareStatement(sql);
            rs = pst.executeQuery();
            Rol rol= new Rol();//creamos primer rol
            ArrayList<Permiso> permisos = new ArrayList<Permiso>(); //creamos primer arreglo de permisos para el primer rol
            String nombre = rs.getString("nombre_r");
            permisos.add(Permiso.valueOf(rs.getString("p.nombre")));
            rol.setNombre(NombreRol.valueOf(nombre));
            while(rs.next()){
                if(nombre.equals(rs.getString("nombre_r"))){
                    Permiso permiso = Permiso.valueOf(rs.getString("nombre_p")); 
                    permisos.add(permiso);
                }
                else{
                    rol.setPermisos(permisos);
                    listadoRoles.add(rol);
                    rol = new Rol();//creo nuevo rol porque ya no estoy en el mismo rol
                    permisos = new ArrayList<Permiso>();//creo nueva lista de permisos para el nuevo rol
                    nombre = rs.getString("nombre_r");
                    permisos.add(Permiso.valueOf(rs.getString("p.nombre")));//aniado el permiso de la tabla en el arreglo permisos
                    rol.setNombre(NombreRol.valueOf(nombre));//seteo nombre de rol
                }
            }
        }catch(Exception ex){
            System.out.println(ex.getMessage());
        }
        finally{
            try{con.close();} catch(Exception ex){System.out.println(ex.getMessage());}
        }
        return listadoRoles;
    }
    
    public Rol obtenerPorId(String nombreRol){
        throw new UnsupportedOperationException("Not supported yet.");
    }
}