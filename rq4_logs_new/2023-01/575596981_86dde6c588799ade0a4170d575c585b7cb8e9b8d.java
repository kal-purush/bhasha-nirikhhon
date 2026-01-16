package EV2.OOP.C_130123;

public class Perro {
    public static void main(String[] args) {
        Perro perro = new Perro("Firulais", 5, "Pastor Alemán");
        System.out.println("Nombre: "+perro.getNombre());
        System.out.println("Edad: "+perro.getEdad());
        System.out.println("Raza: "+perro.getRaza());
        perro.ladrar(perro.getNombre());
        perro.comer("carne");
        System.out.println();

        Perro.ladrar();

        System.out.println();

        Perro perro2 = new Perro();
        perro2.setNombre("Lassie");
        perro2.setEdad(3);
        perro2.setRaza("Pastor Belga");
        System.out.println("Nombre: "+perro2.getNombre());
        System.out.println("Edad: "+perro2.getEdad());
        System.out.println("Raza: "+perro2.getRaza());
        perro2.ladrar(perro2.getNombre());
        perro2.comer("carne");
        System.out.println();

        Perro.ladrar();

        System.out.println();
    }

    // Atributos de la clase
    private String nombre;
    private int edad;
    private String raza;

    // Constructor
    public Perro(){
    }

    // Constructor sobrecargado
    public Perro(String nombre, int edad, String raza) {
        this.nombre = nombre;
        this.edad = edad;
        this.raza = raza;
    }

    // Métodos
    public static void ladrar() {
        System.out.println("Guau Guau!");
    }

    // Método sobrecargado
    public void ladrar(String nombre) {
        System.out.println(nombre+": Guau Guau!");
    }

    public void comer(String comida) {
        System.out.println(nombre + " está comiendo " + comida);
    }

    // Getters y Setters
    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public int getEdad() {
        return edad;
    }

    public void setEdad(int edad) {
        this.edad = edad;
    }

    public String getRaza() {
        return raza;
    }

    public void setRaza(String raza) {
        this.raza = raza;
    }
}