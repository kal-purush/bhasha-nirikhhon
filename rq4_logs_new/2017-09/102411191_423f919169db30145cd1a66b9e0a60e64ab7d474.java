import java.util.*;

public class HistorialEstadoConcurso {
    private Calendar fecha;
    private EstadoConcurso estado;
    private String comentarios;

    public HistorialEstadoConcurso() {
    }

    public Calendar getFecha() {
        return fecha;
    }

    public void setFecha(Calendar fecha) {
        this.fecha = fecha;
    }

    public EstadoConcurso getEstado() {
        return estado;
    }

    public void setEstado(EstadoConcurso estado) {
        this.estado = estado;
    }

    public String getComentarios() {
        return comentarios;
    }

    public void setComentarios(String comentarios) {
        this.comentarios = comentarios;
    }
}