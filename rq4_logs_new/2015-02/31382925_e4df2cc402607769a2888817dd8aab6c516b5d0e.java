package almacendefinitivo;

import java.util.Scanner;

public class Leche extends Producto {
	
	public Leche(Distribuidor distribuidor, String marca, Double precio, String procedencia){
		super(distribuidor, marca, precio, procedencia);
	}
	public Leche(Scanner sc){
		super(sc);
	}
	@Override
	public void mostrarProducto(){
	System.out.println("Leche");
	super.mostrarProducto();	
	}
}