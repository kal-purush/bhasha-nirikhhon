package condicionalesYciclo;
import java.util.Scanner;

public class CondicionalesDeHotel {
	public static void main(String[] args) {
		
		int numeroHabitacionUsuario = 0;
		int numeroHabitacionReservada = 5;
		boolean habitacionDisponible = true;
		boolean reservacion = false;
		boolean habitacionPagada = false;
		int camasExtra = 0;
		boolean membresia = false;

		Scanner entradaUsuario = new Scanner(System.in);
		
		System.out.println("Cuál número de habitación deseeas?: ");
		numeroHabitacionUsuario = entradaUsuario.nextInt();
		
		if(numeroHabitacionUsuario==numeroHabitacionReservada) {
			habitacionDisponible = false;
			System.out.println("Habitación disponible: " + habitacionDisponible);
			reservacion = true;
			System.out.println("Habitación reservada: " + reservacion);
			habitacionPagada = true;
			System.out.println("Habitación pagada: " + habitacionPagada);
			camasExtra = 2;
			System.out.println("Camas extra: " + camasExtra);
			membresia = true;
			System.out.println("Membresía: " + membresia);
		
		}else {		
			System.out.println("Habitación disponible: " + habitacionDisponible);		
			System.out.println("Habitación reservada: " + reservacion);
			System.out.println("Habitación pagada: " + habitacionPagada);
			System.out.println("Camas extra: " + camasExtra);
			System.out.println("Membresía: " + membresia);			
		}	
		entradaUsuario.close();
	}
}