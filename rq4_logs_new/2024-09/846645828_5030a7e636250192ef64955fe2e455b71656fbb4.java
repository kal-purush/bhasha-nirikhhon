package pe.edu.upc.project_security_g06.entities;

import jakarta.persistence.*;

@Entity
@Table(name = "Ciudad")
public class Ciudad {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int idCiudad;

    @Column(name = "nombre_ciudad", nullable = false, length = 50)
    private String nombre_ciudad;

    public Ciudad() { }

    public Ciudad(int idCiudad, String nombre) {
        this.idCiudad = idCiudad;
        this.nombre_ciudad = nombre_ciudad;
    }

    public int getIdCiudad() { return idCiudad; }
    public void setIdCiudad(int idCiudad) {
        this.idCiudad = idCiudad;
    }
    public String getNombre_ciudad() { return nombre_ciudad; }
    public void setNombre_ciudad(String nombre_ciudad) {
        this.nombre_ciudad = nombre_ciudad;
    }
}