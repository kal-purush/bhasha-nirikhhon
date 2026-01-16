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
public class Derechosusuapl {
    
    int id_derechosusuapl;
    String username;
    String nombre_aplicacion;
    String der_insertar;
    String der_editar;
    String der_eliminar;
    String der_imprimir;

    public Derechosusuapl(int id_derechosusuapl, String username, String nombre_aplicacion, String der_insertar, String der_editar, String der_eliminar, String der_imprimir) {
        this.id_derechosusuapl = id_derechosusuapl;
        this.username = username;
        this.nombre_aplicacion = nombre_aplicacion;
        this.der_insertar = der_insertar;
        this.der_editar = der_editar;
        this.der_eliminar = der_eliminar;
        this.der_imprimir = der_imprimir;
    }

    @Override
    public String toString() {
        return "Derechosusuapl{" + "id_derechosusuapl=" + id_derechosusuapl + ", username=" + username + ", nombre_aplicacion=" + nombre_aplicacion + ", der_insertar=" + der_insertar + ", der_editar=" + der_editar + ", der_eliminar=" + der_eliminar + ", der_imprimir=" + der_imprimir + '}';
    }

    public int getId_derechosusuapl() {
        return id_derechosusuapl;
    }

    public void setId_derechosusuapl(int id_derechosusuapl) {
        this.id_derechosusuapl = id_derechosusuapl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getNombre_aplicacion() {
        return nombre_aplicacion;
    }

    public void setNombre_aplicacion(String nombre_aplicacion) {
        this.nombre_aplicacion = nombre_aplicacion;
    }

    public String getDer_insertar() {
        return der_insertar;
    }

    public void setDer_insertar(String der_insertar) {
        this.der_insertar = der_insertar;
    }

    public String getDer_editar() {
        return der_editar;
    }

    public void setDer_editar(String der_editar) {
        this.der_editar = der_editar;
    }

    public String getDer_eliminar() {
        return der_eliminar;
    }

    public void setDer_eliminar(String der_eliminar) {
        this.der_eliminar = der_eliminar;
    }

    public String getDer_imprimir() {
        return der_imprimir;
    }

    public void setDer_imprimir(String der_imprimir) {
        this.der_imprimir = der_imprimir;
    }
    
    public Derechosusuapl() {
    }
   
}