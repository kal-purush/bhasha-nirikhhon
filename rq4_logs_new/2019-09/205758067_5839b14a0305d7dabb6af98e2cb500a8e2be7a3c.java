/*
 * Programa      : PROYECTO PROGRAMACION INTERACTIVA 2019- DVD RENTAL
 * Fecha         : Septiembre-2019
 * Objetivo      : Se declaran atributos, el constructor sin parametros y los métodos get y set de la clase Inventario
 * Programadores : Cristhian Guzman, Juan Martinez, Nathalia Riascos, Vanesa Cifuentes
 * Clase         : Inventario
 */
package Modelo;
import java.sql.Timestamp;

public class Inventario {
    
    private int inventarioID;
    private int peliculaID_inventario;
    private int tiendaID;
    private Timestamp ultimaActualizacion;

    public Inventario() {
    }

    public int getInventarioID() {
        return inventarioID;
    }

    public int getPeliculaID_inventario() {
        return peliculaID_inventario;
    }

    public int getTiendaID() {
        return tiendaID;
    }

    public void setInventarioID(int inventarioID) {
        this.inventarioID = inventarioID;
    }

    public void setPeliculaID_inventario(int peliculaID_inventario) {
        this.peliculaID_inventario = peliculaID_inventario;
    }

    public void setTiendaID(int tiendaID) {
        this.tiendaID = tiendaID;
    }

    public Timestamp getUltimaActualizacion() {
        return ultimaActualizacion;
    }

    public void setUltimaActualizacion(Timestamp ultimaActualizacion) {
        this.ultimaActualizacion = ultimaActualizacion;
    }
    
    
    
}