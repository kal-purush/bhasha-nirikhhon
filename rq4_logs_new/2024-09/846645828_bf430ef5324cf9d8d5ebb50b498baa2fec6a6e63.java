package pe.edu.upc.project_security_g06.dtos;

public class UsuarioDispositivoCountDTO {
    private String nombreUsuario;
    private String apellidoUsuario;
    private Long cantidadDispositivos;

    public String getNombreUsuario() {
        return nombreUsuario;
    }

    public void setNombreUsuario(String nombreUsuario) {
        this.nombreUsuario = nombreUsuario;
    }

    public String getApellidoUsuario() {
        return apellidoUsuario;
    }

    public void setApellidoUsuario(String apellidoUsuario) {
        this.apellidoUsuario = apellidoUsuario;
    }

    public Long getCantidadDispositivos() {
        return cantidadDispositivos;
    }

    public void setCantidadDispositivos(Long cantidadDispositivos) {
        this.cantidadDispositivos = cantidadDispositivos;
    }
}