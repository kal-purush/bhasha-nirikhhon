public class Agencia {

    private String nombre;
    private boolean autosNuevos;

    // constructor
    public Agencia() {
        nombre = "Sin nombre";
        autosNuevos = true;
    }

    // métodos get
    public String getNombre() {
        return nombre;
    }

    public boolean getAutosNuevos() {
        return autosNuevos;
    }

    // métodos set
    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public void setAutosNuevos(boolean autosNuevos) {
        this.autosNuevos = autosNuevos;
    }
}