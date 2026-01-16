package pe.edu.upc.project_security_g06.dtos;

import pe.edu.upc.project_security_g06.entities.Ciudad;

public class DistritoDTO {
    private int idDistrito;
    private String nombre_Distrito;
    private Ciudad ciudad;

    public int getIdDistrito() { return idDistrito; }
    public void setIdDistrito(int idDistrito) { this.idDistrito = idDistrito; }
    public String getNombre_Distrito() { return nombre_Distrito; }
    public void setNombre_Distrito(String nombre_Distrito) { this.nombre_Distrito = nombre_Distrito; }
    public Ciudad getCiudad() { return ciudad; }
    public void setCiudad(Ciudad ciudad) {
        this.ciudad = ciudad;
    }
}