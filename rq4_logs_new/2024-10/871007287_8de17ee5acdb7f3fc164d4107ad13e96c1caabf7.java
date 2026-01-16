package main;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class SistemaAlquiler {

    public static class DataStore {
        public static List<Vehiculo> flotaVehiculos = new ArrayList<>();
        public static List<Cliente> clientes = new ArrayList<>();
        public static List<Reserva> reservas = new ArrayList<>();
    }

    public class Administrador {

        public void login() {
            Scanner scanner = new Scanner(System.in);
            System.out.println("\n--- Bienvenido, Administrador ---");
            menuAdmin(scanner);
        }

        private void menuAdmin(Scanner scanner) {
            while (true) {
                System.out.println("\n--- Menú Administrador ---");
                System.out.println("1. Añadir Vehículo");
                System.out.println("2. Ver Vehículos Disponibles");
                System.out.println("3. Ver Todas las Reservas");
                System.out.println("4. Salir");
                System.out.print("Seleccione una opción: ");

                int opcion = 0;
                try {
                    opcion = Integer.parseInt(scanner.nextLine());
                } catch (NumberFormatException e) {
                    System.out.println("Entrada inválida. Por favor, ingrese un número.");
                    continue;
                }

                switch (opcion) {
                    case 1:
                        añadirVehiculo(scanner);
                        break;
                    case 2:
                        verVehiculosDisponibles();
                        break;
                    case 3:
                        verTodasLasReservas();
                        break;
                    case 4:
                        System.out.println("Saliendo del menú del administrador...");
                        return;
                    default:
                        System.out.println("Opción no válida. Intente nuevamente.");
                }
            }
        }

        private void añadirVehiculo(Scanner scanner) {
            System.out.println("\n--- Añadir Vehículo ---");
            System.out.println("Tipos de Vehículos:");
            System.out.println("1. Auto");
            System.out.println("2. Moto");
            System.out.println("3. Camioneta");
            System.out.println("4. Autobús");
            System.out.print("Seleccione el tipo de vehículo: ");

            int tipo = 0;
            try {
                tipo = Integer.parseInt(scanner.nextLine());
            } catch (NumberFormatException e) {
                System.out.println("Entrada inválida. Debe ser un número.");
                return;
            }

            System.out.print("Ingrese ID del Vehículo: ");
            String idVehiculo = scanner.nextLine().trim();

            for (Vehiculo v : DataStore.flotaVehiculos) {
                if (v.getIdVehiculo().equals(idVehiculo)) {
                    System.out.println("El ID del vehículo ya existe. Por favor, elija otro ID.");
                    return;
                }
            }

            System.out.print("Ingrese Marca: ");
            String marca = scanner.nextLine().trim();
            System.out.print("Ingrese Modelo: ");
            String modelo = scanner.nextLine().trim();
            System.out.print("Ingrese Año: ");
            int año = 0;
            try {
                año = Integer.parseInt(scanner.nextLine());
            } catch (NumberFormatException e) {
                System.out.println("Año inválido.");
                return;
            }

            System.out.print("Ingrese Costo Diario: ");
            double costoDiario = 0.0;
            try {
                costoDiario = Double.parseDouble(scanner.nextLine());
            } catch (NumberFormatException e) {
                System.out.println("Costo diario inválido.");
                return;
            }

            Vehiculo vehiculo = null;
            switch (tipo) {
                case 1:
                    System.out.print("Ingrese Tipo de Combustible (Gasolina/Diésel/Eléctrico): ");
                    String tipoCombustible = scanner.nextLine().trim();
                    vehiculo = new Auto(idVehiculo, marca, modelo, año, costoDiario, tipoCombustible);
                    break;
                case 2:
                    System.out.print("Ingrese Cilindrada (cc): ");
                    int cilindrada = 0;
                    try {
                        cilindrada = Integer.parseInt(scanner.nextLine());
                    } catch (NumberFormatException e) {
                        System.out.println("Cilindrada inválida.");
                        return;
                    }
                    vehiculo = new Moto(idVehiculo, marca, modelo, año, costoDiario, cilindrada);
                    break;
                case 3:
                    System.out.print("Ingrese Capacidad de Carga (kg): ");
                    double capacidadCarga = 0.0;
                    try {
                        capacidadCarga = Double.parseDouble(scanner.nextLine());
                    } catch (NumberFormatException e) {
                        System.out.println("Capacidad de carga inválida.");
                        return;
                    }
                    vehiculo = new Camioneta(idVehiculo, marca, modelo, año, costoDiario, capacidadCarga);
                    break;
                case 4:
                    System.out.print("Ingrese Capacidad de Pasajeros: ");
                    int capacidadPasajeros = 0;
                    try {
                        capacidadPasajeros = Integer.parseInt(scanner.nextLine());
                    } catch (NumberFormatException e) {
                        System.out.println("Capacidad de pasajeros inválida.");
                        return;
                    }
                    vehiculo = new Autobus(idVehiculo, marca, modelo, año, costoDiario, capacidadPasajeros);
                    break;
                default:
                    System.out.println("Tipo de vehículo no válido.");
                    return;
            }

            DataStore.flotaVehiculos.add(vehiculo);
            System.out.println("Vehículo añadido exitosamente:");
            System.out.println(vehiculo);
        }

        private void verVehiculosDisponibles() {
            System.out.println("\n--- Vehículos Disponibles ---");
            boolean hayDisponibles = false;
            for (Vehiculo v : DataStore.flotaVehiculos) {
                if (v.isDisponible()) {
                    System.out.println(v);
                    hayDisponibles = true;
                }
            }
            if (!hayDisponibles) {
                System.out.println("No hay vehículos disponibles en este momento.");
            }
        }

        private void verTodasLasReservas() {
            System.out.println("\n--- Todas las Reservas ---");
            if (DataStore.reservas.isEmpty()) {
                System.out.println("No hay reservas realizadas.");
                return;
            }
            for (Reserva r : DataStore.reservas) {
                System.out.println(r);
            }
        }
    }

    public static void main(String[] args) {
        SistemaAlquiler sistema = new SistemaAlquiler();
        Scanner scanner = new Scanner(System.in);
        Administrador admin = sistema.new Administrador();

        while (true) {
            System.out.println("\n=== Sistema de Alquiler de Vehículos ===");
            System.out.println("Seleccione una opción:");
            System.out.println("1. Administrador");
            System.out.println("2. Cliente (Ver Vehículos y Reservar)");
            System.out.println("3. Salir");
            System.out.print("Opción: ");

            int opcion = 0;
            try {
                opcion = Integer.parseInt(scanner.nextLine());
            } catch (NumberFormatException e) {
                System.out.println("Entrada inválida. Por favor, ingrese un número.");
                continue;
            }

            switch (opcion) {
                case 1:
                    admin.login();
                    break;
                case 2:
                    break;
                case 3:
                    System.out.println("Gracias por usar el Sistema de Alquiler de Vehículos. ¡Hasta luego!");
                    scanner.close();
                    return;
                default:
                    System.out.println("Opción no válida. Intente nuevamente.");
            }
        }
    }
}