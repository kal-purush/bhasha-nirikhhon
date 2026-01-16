package pe.edu.upc.project_security_g06.dtos;

public class UsuarioConteoAlerEnferDTO {
    private String usuario;
    private int cantidad_alergias;
    private int cantidad_enfermedades;
    private String tipos_enfermedades;

    public String getUsuario() {return usuario;}
    public void setUsuario(String usuario) {this.usuario = usuario;}
    public int getCantidad_alergias() {return cantidad_alergias;}
    public void setCantidad_alergias(int cantidad_alergias) {this.cantidad_alergias = cantidad_alergias;}
    public int getCantidad_enfermedades() {return cantidad_enfermedades;}
    public void setCantidad_enfermedades(int cantidad_enfermedades) {this.cantidad_enfermedades = cantidad_enfermedades;}
    public String getTipos_enfermedades() {return tipos_enfermedades;}
    public void setTipos_enfermedades(String tipos_enfermedades) {this.tipos_enfermedades = tipos_enfermedades;}
}