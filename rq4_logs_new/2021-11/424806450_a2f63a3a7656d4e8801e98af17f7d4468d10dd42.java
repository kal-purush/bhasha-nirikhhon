/**
 * Clase para crear un menu con  switch
 * para los autos
 *
 * @author Daniela Pereyra
 * @version 1,0 (12-11-21)
 * */
import java.util.Scanner;

public class AutoSwitch{

    public static void main(String[] args){


	    // Imprimimos bienvenida para el usuario
	    System.out.println("\n*Bienvenido a la Agencia de Autos Ferrary");

	    Scanner sc = new Scanner(System.in);


	    boolean salir = false;
	    int opcion; 

	    ClaseAuto auto1 = new ClaseAuto();

	    while (!salir){
	    System.out.println("\n*Menú De Autos De La Agencia Ferrari* \n [1] Deportivos \n [2] Caminonetas \n [3] Lo Más Nuevo");
	    System.out.println("Por favor ingrese un número de acuerdo a la opción: ");
	    opcion = sc.nextInt();

	    /**
	     * Switch con cada uno de los casos para 
	     * diferentes opciones del menú
	     * */
	    switch (opcion) {
	    case 1:
		System.out.println("\n*Bienvenido a la sección de Autos Deportivos");
		System.out.println("\nEn esta sección tenemos el mejor auto deportivo del año");
		// Uso del método get para devoler el valor e imprimimos
		System.out.println("\nEl tipo de auto es "+auto1.getTipo());
		System.out.println("De color "+auto1.getColor());
		System.out.println("Modelo "+auto1.getModelo());
		// Imprimimos el método toString en el objeto auto1
		System.out.println(auto1.toString());
		
		//Uso del método set para cambial el valor e imprimimos
		Scanner si = new Scanner(System.in);
		System.out.println("Desea cambiar alguna caracteristica?, si[1] / no [2]");
		int x = si.nextInt();
		if (x == 1){
			    System.out.println("Ingrese el Tipo");
			    auto1.setTipo(si.nextLine());
			    auto1.setTipo(si.nextLine());
			    System.out.println("Ingrese el Color");
			    auto1.setColor(si.nextLine());
			    System.out.println("Ingrese el Modelo");
			    auto1.setModelo(si.nextLine());
			    // Uso del método get para devolver un valor e imprimimos
			    System.out.println("\n\nEl tipo de auto es "+auto1.getTipo());
			    System.out.println("De color "+auto1.getColor());
			    System.out.println("Modelo "+auto1.getModelo());
			    // Imprimimos el método toString en el objeto auto1
			    System.out.println(auto1.toString());
		} else {
		    System.out.println("Muy bien, proseguiremos con su compra");
		}
	    }
	    break;
	    case 2:
		System.out.println("\n*Bienvenido a la sección de Camionetas");
		System.out.println("\nEn esta sección encontraras lo ultimo de camionetas");
		
	    }
	    salir = true;
    }
}
    
    	
	
	