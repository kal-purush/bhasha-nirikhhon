/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package pe.edu.pucp.gdptalento.core.dao;
import java.util.ArrayList;
import pe.edu.pucp.gdptalento.core.model.Rol;


/**
 *
 * @author USER
 */
public interface RolDAO {
    int insertar(Rol rol);
    int modificar(Rol rol);
    int eliminar(Rol rol);
    ArrayList<Rol> listarTodas();
    Rol obtenerPorId(String nombreRol);
}