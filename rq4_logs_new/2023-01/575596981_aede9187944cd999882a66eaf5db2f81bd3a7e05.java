package EV2.OOP.C_120123;

public class Main {
    public static void main(String[] args) {
        Vehiculo vehiculo1 = new Vehiculo();

        vehiculo1.setMarca("Ford");
        vehiculo1.setModelo("Fiesta");
        vehiculo1.setNumPasajeros(5);
        vehiculo1.setCapacidadCombustible(50);
        vehiculo1.setConsumo(6.3);

        System.out.println("Marca: " + vehiculo1.getMarca());
        System.out.println("Modelo: " + vehiculo1.getModelo());
        System.out.println("Número de pasajeros: " + vehiculo1.getNumPasajeros());
        System.out.println("Capacidad de combustible: " + vehiculo1.getCapacidadCombustible()+" L");
        System.out.println("Consumo: " + vehiculo1.getConsumo()+"L/100km");

        Vehiculo vehiculo2 = new Vehiculo("Alfa", "147", 5, 45, 5.5);

        System.out.println("Marca: " + vehiculo2.getMarca());
        System.out.println("Modelo: " + vehiculo2.getModelo());
        System.out.println("Número de pasajeros: " + vehiculo2.getNumPasajeros());
        System.out.println("Capacidad de combustible: " + vehiculo2.getCapacidadCombustible()+" L");
        System.out.println("Consumo: " + vehiculo2.getConsumo()+"L/100km");
    }
}