package pe.edu.upc.project_security_g06.dtos;

import pe.edu.upc.project_security_g06.entities.Distrito;

import java.time.LocalDate;

public class UbicacionDTO {
    private int IdUbicacion;
    private double longitud;
    private double latitud;
    private LocalDate fechaUbicacion;
    private Distrito distrito;

    public int getIdUbicacion() { return IdUbicacion; }
    public void setIdUbicacion(int IdUbicacion) { this.IdUbicacion = IdUbicacion; }
    public double getLongitud() { return longitud; }
    public void setLongitud(double longitud) { this.longitud = longitud; }
    public double getLatitud() { return latitud; }
    public void setLatitud(double latitud) { this.latitud = latitud; }
    public LocalDate getFechaUbicacion() { return fechaUbicacion; }
    public void setFechaUbicacion(LocalDate fechaUbicacion) { this.fechaUbicacion = fechaUbicacion;}
    public Distrito getDistrito() { return distrito; }
    public void setDistrito(Distrito distrito) {this.distrito = distrito;}
}