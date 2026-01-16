package pe.edu.upc.project_security_g06.dtos;

public class DispositivoContactoAutoridadDTO {
    private String NombreDispositivo;
    private String NombreAutoridad;
    private int telefonoAutoridad;

    public String getNombreDispositivo() {
        return NombreDispositivo;
    }

    public void setNombreDispositivo(String nombreDispositivo) {
        NombreDispositivo = nombreDispositivo;
    }

    public String getNombreAutoridad() {
        return NombreAutoridad;
    }

    public void setNombreAutoridad(String nombreAutoridad) {
        NombreAutoridad = nombreAutoridad;
    }

    public int getTelefonoAutoridad() {
        return telefonoAutoridad;
    }

    public void setTelefonoAutoridad(int telefonoAutoridad) {
        this.telefonoAutoridad = telefonoAutoridad;
    }
}