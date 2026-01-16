/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Controlador.seguridad;

/**
 *
 * @author visitante
 */
public class Perfil {
    
    int id_perfil;
    String nombre_perfil;
    String estatus_perfil;

    public Perfil(int id_perfil, String nombre_perfil, String estatus_perfil) {
        this.id_perfil = id_perfil;
        this.nombre_perfil = nombre_perfil;
        this.estatus_perfil = estatus_perfil;
    }

    @Override
    public String toString() {
        return "Perfil{" + "id_perfil=" + id_perfil + ", nombre_perfil=" + nombre_perfil + ", estatus_perfil=" + estatus_perfil + '}';
    }
    
    public int getId_perfil() {
        return id_perfil;
    }

    public void setId_perfil(int id_perfil) {
        this.id_perfil = id_perfil;
    }

    public String getNombre_perfil() {
        return nombre_perfil;
    }

    public void setNombre_perfil(String nombre_perfil) {
        this.nombre_perfil = nombre_perfil;
    }

    public String getEstatus_perfil() {
        return estatus_perfil;
    }

    public void setEstatus_perfil(String estatus_perfil) {
        this.estatus_perfil = estatus_perfil;
    }

    public Perfil() {
    }
   
}