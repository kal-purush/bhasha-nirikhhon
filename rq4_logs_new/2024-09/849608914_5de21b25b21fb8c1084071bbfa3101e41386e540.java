package pe.edu.upc.waba.dtos.QuerysDTO;

public class RecursoQ1DTO {
    private String nombreProfesor;
    private double horasReservadas;

    public String getNombreProfesor() {
        return nombreProfesor;
    }

    public void setNombreProfesor(String nombreProfesor) {
        this.nombreProfesor = nombreProfesor;
    }

    public double getHorasReservadas() {
        return horasReservadas;
    }

    public void setHorasReservadas(double horasReservadas) {
        this.horasReservadas = horasReservadas;
    }
}