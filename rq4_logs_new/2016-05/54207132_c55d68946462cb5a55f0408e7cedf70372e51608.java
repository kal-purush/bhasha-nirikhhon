package ceindetec.mesabar.Models;

public class ModelInfoComentariosSucursal {

    // Atributos
    private String avatarComentario;
    private String tituloComentario;
    private String textoComentario;
    private String puntuacionComentario;
    private String fechaComentario;
    private String horaComentario;

    public ModelInfoComentariosSucursal() {
    }

    public ModelInfoComentariosSucursal(String avatarComentario, String tituloComentario, String textoComentario, String puntuacionComentario, String fechaComentario, String horaComentario) {
        this.avatarComentario = avatarComentario;
        this.tituloComentario = tituloComentario;
        this.textoComentario = textoComentario;
        this.puntuacionComentario = puntuacionComentario;
        this.fechaComentario = fechaComentario;
        this.horaComentario = horaComentario;
    }

    public String getAvatarComentario() {
        return avatarComentario;
    }

    public void setAvatarComentario(String avatarComentario) {
        this.avatarComentario = avatarComentario;
    }

    public String getTituloComentario() {
        return tituloComentario;
    }

    public void setTituloComentario(String tituloComentario) {
        this.tituloComentario = tituloComentario;
    }

    public String getTextoComentario() {
        return textoComentario;
    }

    public void setTextoComentario(String textoComentario) {
        this.textoComentario = textoComentario;
    }

    public String getPuntuacionComentario() {
        return puntuacionComentario;
    }

    public void setPuntuacionComentario(String puntuacionComentario) {
        this.puntuacionComentario = puntuacionComentario;
    }

    public String getFechaComentario() {
        return fechaComentario;
    }

    public void setFechaComentario(String fechaComentario) {
        this.fechaComentario = fechaComentario;
    }

    public String getHoraComentario() {
        return horaComentario;
    }

    public void setHoraComentario(String horaComentario) {
        this.horaComentario = horaComentario;
    }

}