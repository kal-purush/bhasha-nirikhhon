/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Modelos;

/**
 *
 * @author osval
 */
public class Reservaciones {
    private int idReservaciones;
    private String fechaEntrega;
    private String fechaSalida;
    private String nombreUsuario;
    private int idCliente;
    private int idHabitacion;

    public Reservaciones(int idReservaciones, String fechaEntrega, String fechaSalida, String nombreUsuario, int idCliente, int idHabitacion) {
        this.idReservaciones = idReservaciones;
        this.fechaEntrega = fechaEntrega;
        this.fechaSalida = fechaSalida;
        this.nombreUsuario = nombreUsuario;
        this.idCliente = idCliente;
        this.idHabitacion = idHabitacion;
    }

    public int getIdReservaciones() {
        return idReservaciones;
    }

    public void setIdReservaciones(int idReservaciones) {
        this.idReservaciones = idReservaciones;
    }

    public String getFechaEntrega() {
        return fechaEntrega;
    }

    public void setFechaEntrega(String fechaEntrega) {
        this.fechaEntrega = fechaEntrega;
    }

    public String getFechaSalida() {
        return fechaSalida;
    }

    public void setFechaSalida(String fechaSalida) {
        this.fechaSalida = fechaSalida;
    }

    public String getNombreUsuario() {
        return nombreUsuario;
    }

    public void setNombreUsuario(String nombreUsuario) {
        this.nombreUsuario = nombreUsuario;
    }

    public int getIdCliente() {
        return idCliente;
    }

    public void setIdCliente(int idCliente) {
        this.idCliente = idCliente;
    }

    public int getIdHabitacion() {
        return idHabitacion;
    }

    public void setIdHabitacion(int idHabitacion) {
        this.idHabitacion = idHabitacion;
    }
    
    

}