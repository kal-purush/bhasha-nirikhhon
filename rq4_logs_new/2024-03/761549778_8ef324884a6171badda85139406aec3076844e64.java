package tec.poo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Scanner;

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        ApplicationContext context = new ApplicationContext();
        RegistroEmpleados registro = context.getRegistroEmpleados();
        Scanner scanner = new Scanner(System.in);

        while (true) {
            logger.info("1. Registrar nuevo empleado");
            logger.info("2. Mostrar detalles de un empleado");
            logger.info("3. Salir");
            logger.info("Elige una opción: ");
            int opcion = scanner.nextInt();

            switch (opcion) {
                case 1:
                    logger.info("Introduce el ID del empleado: ");
                    int id = scanner.nextInt();
                    logger.info("Introduce el nombre del empleado: ");
                    String nombre = scanner.next();
                    logger.info("Introduce el apellido del empleado: ");
                    String apellido = scanner.next();
                    logger.info("Introduce el cargo del empleado: ");
                    String cargo = scanner.next();
                    Empleado empleado = new Empleado(id, nombre, apellido, cargo);
                    registro.registrarNuevoEmpleado(empleado);
                    logger.info("Empleado registrado con éxito.");
                    break;
                case 2:
                    logger.info("Introduce el ID del empleado: ");
                    int idBusqueda = scanner.nextInt();
                    try {
                        registro.mostrarDetallesEmpleado(idBusqueda);
                    } catch (Exception e) {
                        logger.error("Error: " + e.getMessage());
                    }
                    break;
                case 3:
                    logger.info("Saliendo...");
                    scanner.close();
                    System.exit(0);
                default:
                    logger.info("Opción no válida. Por favor, elige una opción del 1 al 3.");
            }
        }
    }
}