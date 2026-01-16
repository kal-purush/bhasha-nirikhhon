import java.util.Scanner;
public class Menu{
    public static void main(String[] args){
	
      	Scanner sc = new Scanner(System.in);	

	System.out.println("Bienvenido a AGENCIA MORE");

	int opcion;
	boolean salida = false;
	
	    System.out.println("1. Autos mas vendidos");
	    System.out.println("2. Autos disponibles");
	    System.out.println("3. Seminuevos");
	    System.out.println("4. Promociones");
	    System.out.println("5. Vendedores");
	    System.out.println("6. Clientes");
      	    System.out.println("7. Salir");
	    System.out.print("Introducir el numero de la accion que desea realizar: ");
	    opcion = sc.nextInt();