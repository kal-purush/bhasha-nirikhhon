package pruevaEnlase;

public class Main {

	public static void main(String[] args) {
       
        Persona persona1 = new Persona("Juan", 25);
        Persona persona2 = new Persona("Ana", 30);

      
        persona1.mostrarInfo();
        persona2.mostrarInfo();

       
        persona1.setEdad(26);
        System.out.println("Nueva edad de " + persona1.getNombre() + ": " + persona1.getEdad());
    }
}