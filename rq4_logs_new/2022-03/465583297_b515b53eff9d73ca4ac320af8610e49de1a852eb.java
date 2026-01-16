package Vistas;

import java.util.InputMismatchException;
import java.util.Scanner;

import static Helpers.clearScreen.clearScreen;

public class MenuPrincipal {
    public void MenuInicial (){
        System.out.println("Bienvenido! \nComo le podemos ayudar?\n");
        System.out.println("1. Bicicletas");
        System.out.println("2. Equipos de proteccion");
        System.out.println("3. Repuestos");
        System.out.println("4. Salir");
    }

    public void menu() throws InterruptedException {

        Scanner input = new Scanner(System.in);
        int option;

        do {
            MenuInicial();

            try {
                System.out.println("Seleccione una opcion: ");
                option = input.nextInt();
                switch (option){
                    case 1:
                        //send to bicicletas menu
                        break;
                    case 2:
                        //send to quipos de proteccion menu
                        break;
                    case 3:
                        //Send to repuestos
                        break;
                    case 4:
                        break;
                    case 5:
                        //if product on cart then move to checkout menu, else continue
                        continue;
                    default:
                        clearScreen();
                        System.out.println("La opcion no es valida. Por favor ingrese una opcion de nuevo.");
                        Thread.sleep(3000);
                        clearScreen();
                }
            }catch (InputMismatchException e){
                System.out.println("Debes ingresar un numero valido");
                option = input.nextInt();
            }

        } while (option != 4);
    }
}