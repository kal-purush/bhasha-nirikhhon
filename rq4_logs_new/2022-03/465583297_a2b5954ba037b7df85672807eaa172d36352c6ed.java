package Vistas;

import Helpers.IngresarOpcionValida;
import controladores.AdministradorClientes;

import java.util.Scanner;

public class MenuAdministrarClientes extends AdministradorClientes {
    private Scanner scanner = new Scanner(System.in);

    public String administrarClientes() throws InterruptedException {

        String opcion = "";
        String siguienteProceso = "";
        boolean finMenu = false;

        System.out.println("\nMenu Principal/Administrar Clientes");
        System.out.println("----------------------------------------");

        do
        {
            try{
                System.out.println("\n\t-> Administración de clientes\n");
                System.out.println("Selecciona una opción");
                System.out.println("1. Agregar cliente");
                System.out.println("2. Modificar cliente");
                System.out.println("3. Deshabilitar/Habilitar cliente");
                System.out.println("\n4. Salir a menu principal");
                System.out.println("5. Finalizar ejecucion");

                opcion = scanner.nextLine();

                if(Integer.parseInt(opcion) < 1 || Integer.parseInt(opcion) > 5){
                    IngresarOpcionValida.imprimir();
                    continue;
                }

                switch (opcion)
                {
                    case "1"://Agregar Producto
                        siguienteProceso = "agregarCliente";
                        break;
                    case "2"://Modificar Producto
                        siguienteProceso = "modificarCliente";
                        break;
                    case "3"://Deshabilitar/Habilitar producto
                        siguienteProceso = "deshabilitarHabilitarCliente";
                        break;
                    case "4"://Salir a menu principal
                        siguienteProceso = "menuPrincipal";
                        break;
                    case "5"://Salir
                        siguienteProceso = "finEjecucion";
                        System.out.println("\nGracias por preferirnos...");
                        break;
                }
                finMenu = true;
            }catch (Exception e)
            {
                IngresarOpcionValida.imprimir();
            }
        }while (finMenu == false);
        scanner.close();
        return siguienteProceso;
    }
}