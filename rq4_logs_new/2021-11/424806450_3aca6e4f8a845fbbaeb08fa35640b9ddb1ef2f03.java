public class Vendedor{

    private String nombre;
    private String n_devendedor;
    private double rating;
    private String animo;
    private double sueldo;

    public Vendedor(){
        this.nombre = "Luis";
        this.n_devendedor = "74";
        this.rating = 8.6;
        this.animo = "cansado";
        this.sueldo = 1290.00;
    }

    public Vendedor(String nombre, String n_devendedor, double rating, String animo, double sueldo){
        this.nombre = nombre;
        this.n_devendedor = n_devendedor;
        this.rating = rating;
        this.animo = animo;
        this.sueldo = sueldo;
    }

    public String getNombre(){
        return this.nombre;
    }
    public String getN_devendedor(){
        return this.n_devendedor;
    }
    public double getRating(){
        return this.rating;
    }
    public String getAnimo(){
        return this.animo;
    }
    public double getSueldo(){
        return this.sueldo;
    }

    public void setNombre(String nombre){
        this.nombre = nombre;
    }
    public void setN_devendedor(String n_devendedor){
        this.n_devendedor = n_devendedor;
    }
    public void setRating(double rating){
        this.rating = rating;
    }
    public void setAnimo(String animo){
        this.animo = animo;
    }
    public void setSueldo(){
        this.sueldo = sueldo;
    }

    public String toString(){
        return "El empleado que lo atenderá se llama " + this.nombre + ", su número de vendedor es " + this.n_devendedor + ",\n" +
        "su rating es de " + this.rating + ", el día de hoy se encuentra " + this.animo + " y esta encantado de atenderle."; 
    }

}