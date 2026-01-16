package pe.edu.upc.project_security_g06.entities;

import jakarta.persistence.*;


@Entity
@Table(name = "Contacto_Emergencia")
public class Contacto_Emergencia {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int IdContacto;
    @Column(name = "nombre_contacto", nullable = false, length = 40)
    private String nombre_contacto;
    @Column(name = "relacion_contacto", nullable = false, length = 40)
    private String relacion_contacto;
    @Column(name = "num_telefono_contacto", nullable = false)
    private int num_telefono_contacto;

    public Contacto_Emergencia() {
    }

    public Contacto_Emergencia(int IdContacto,String nombre_contacto,String relacion_contacto,int num_telefono_contacto) {
        this.IdContacto = IdContacto;
        this.nombre_contacto = nombre_contacto;
        this.relacion_contacto = relacion_contacto;
        this.num_telefono_contacto = num_telefono_contacto;
    }

    public int getIdContacto() {
        return IdContacto;
    }
    public void setIdContacto(int IdContacto) {
        this.IdContacto = IdContacto;
    }
    public String getNombre_contacto() {
        return nombre_contacto;
    }
    public void setNombre_contacto(String nombre_contacto) {
        this.nombre_contacto = nombre_contacto;
    }
    public String getRelacion_contacto() {
        return relacion_contacto;
    }
    public void setRelacion_contacto(String relacion_contacto) {
        this.relacion_contacto = relacion_contacto;
    }
    public int getNum_telefono_contacto() {
        return num_telefono_contacto;
    }
    public void setNum_telefono_contacto(int num_telefono_contact) {
        this.num_telefono_contacto = num_telefono_contact;
    }

}