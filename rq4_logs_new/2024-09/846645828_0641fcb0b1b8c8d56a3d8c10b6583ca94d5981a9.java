package pe.edu.upc.project_security_g06.dtos;

public class UsuarioAlergiaDTO {
    private String nombreUsuario;
    private String apellidoUsuario;
    private String nombreAlergia;
    private String descripcionAlergia;
    private String causaAlergia;

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

    public String getNombreAlergia() {
        return nombreAlergia;
    }

    public void setNombreAlergia(String nombreAlergia) {
        this.nombreAlergia = nombreAlergia;
    }

    public String getDescripcionAlergia() {
        return descripcionAlergia;
    }

    public void setDescripcionAlergia(String descripcionAlergia) {
        this.descripcionAlergia = descripcionAlergia;
    }

    public String getCausaAlergia() {
        return causaAlergia;
    }

    public void setCausaAlergia(String causaAlergia) {
        this.causaAlergia = causaAlergia;
    }
}