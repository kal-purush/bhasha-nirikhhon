package pe.edu.upc.project_security_g06.dtos;

public class UsuarioContactosDTO {
    private int IdUsario;
    private String us_nombre;
    private String us_apellido;
    private String nombre_dispositivo;
    private String nombre_contacto;
    private int num_telefono_contacto;
    public int numeTelefono_contac_Auto;


    public int getIdUsario() {
        return IdUsario;
    }
    public void setIdUsario(int IdUsario) {
        this.IdUsario = IdUsario;
    }
    public String getUs_nombre(){
        return us_nombre;
    }
    public void setUs_nombre(String us_nombre){
        this.us_nombre = us_nombre;
    }
    public String getUs_apellido(){
        return us_apellido;
    }
    public void setUs_apellido(String us_apellido){
        this.us_apellido = us_apellido;
    }
    public String getNombre_dispositivo(){
        return nombre_dispositivo;
    }
    public void setNombre_dispositivo(String nombre_dispositivo){
        this.nombre_dispositivo = nombre_dispositivo;
    }
    public String getNombre_contacto(){
        return nombre_contacto;
    }
    public void setNombre_contacto(String nombre_contacto){
        this.nombre_contacto = nombre_contacto;
    }
    public int getNum_telefono_contacto(){
        return num_telefono_contacto;
    }
    public void setNum_telefono_contacto(int num_telefono_contacto) {
        this.num_telefono_contacto = num_telefono_contacto;
    }
    public int getNumeTelefono_contac_Auto() {
        return numeTelefono_contac_Auto;
    }
    public void setNumeTelefono_contac_Auto() {
        this.numeTelefono_contac_Auto = numeTelefono_contac_Auto;
    }

}