import java.time.LocalDate;
import java.util.List;

public class Entrevista {
    private int id;
    private LocalDate fecha;
    private Postulante postulante;
    private EstadoEntrevista estado;
    private String feedback;
    private List<Integer> puntuaciones;
    private double puntuacionFinal;

    public Entrevista(int id, LocalDate fecha, Postulante postulante, EstadoEntrevista estado, String feedback, List<Integer> puntuaciones) {
        this.id = id;
        this.fecha = fecha;
        this.postulante = postulante;
        this.estado = estado;
        this.feedback = feedback;
        this.puntuaciones = puntuaciones;
        this.puntuacionFinal = calcularPuntuacionFinal();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public LocalDate getFecha() {
        return fecha;
    }

    public void setFecha(LocalDate fecha) {
        this.fecha = fecha;
    }

    public Postulante getPostulante() {
        return postulante;
    }

    public void setPostulante(Postulante postulante) {
        this.postulante = postulante;
    }

    public EstadoEntrevista getEstado() {
        return estado;
    }

    public void setEstado(EstadoEntrevista estado) {
        this.estado = estado;
    }

    public String getFeedback() {
        return feedback;
    }

    public void setFeedback(String feedback) {
        this.feedback = feedback;
    }

    public double getPuntuacionFinal() {
        return puntuacionFinal;
    }

    // Método para calcular la puntuación final (promedio de las puntuaciones)
    private double calcularPuntuacionFinal() {
        if (puntuaciones == null || puntuaciones.isEmpty()) {
            return 0;
        }

        double suma = 0;
        for (int puntuacion : puntuaciones) {
            suma += puntuacion;
        }

        return suma / puntuaciones.size();
    }

}