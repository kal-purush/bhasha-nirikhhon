package SistemaGestionHospital;

public class CitasMedicas {
    private String fecha;
    private String hora;
    private String motivoDeCita;
    private String contador;


    public CitasMedicas(String fecha, String hora, String motivo, String estado, Paciente paciente, Doctor doctor) {
        this.fecha = fecha;
        this.hora = hora;
        this.motivoDeCita = motivo;
        this.contador = contador;

    }

    public String getFecha() {
        return fecha;
    }

    public String getHora() {
        return hora;
    }

    public String getMotivoDeCita() {
        return motivoDeCita;
    }

    public String getContador() {
        return contador;
    }

    public void setFecha(String fecha) {
        this.fecha = fecha;
    }

    public void setHora(String hora) {
        this.hora = hora;
    }

    public void setMotivoDeCita(String motivo) {
        this.motivoDeCita = motivo;
    }

    public void setEstado(String estado) {
        this.contador = contador;
    }
    //Relaciones
    private Paciente paciente;
    private Doctor doctor;



}