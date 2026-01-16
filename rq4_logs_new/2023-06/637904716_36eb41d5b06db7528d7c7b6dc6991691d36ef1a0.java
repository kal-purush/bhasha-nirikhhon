package modelo;

public class Producto {

    protected int id;
    protected String nombre;
    protected  float precio;
    protected  String categoria;
    protected  int existencia;

    public Producto(int id, String nombre, float precio, String categoria, int existencia) {

        setId(id);
        setNombre(nombre);
        setPrecio(precio);
        setCategoria(categoria);
        setExistencia(existencia);

    }

    public void setId(int id) {

        this.id = id;

    }

    public void setNombre(String nombre) {

        this.nombre = nombre;

    }

    public void setPrecio(float precio){

        this.precio = precio;

    }

    public void setCategoria(String categoria){

        this.categoria = categoria;

    }

    public void setExistencia(int existencia){

        this.existencia = existencia;

    }

    public int getId() {

        return id;

    }

    public String getNombre() {

        return nombre;

    }

    public String getPrecio() {

        return precio;

    }

    public String getCategoria() {

        return categoria;

    }

    public String getExistencia() {

        return existencia;

    }

}