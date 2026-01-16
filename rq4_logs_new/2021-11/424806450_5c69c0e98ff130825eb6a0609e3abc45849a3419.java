import java.util.Scanner;

public class Agencia{
    public static void main(String[] args){
	Scanner sc = new Scanner(System.in);

	System.out.println("****BIENVENIDO A LA AGENCIA LOS CHICOS QUE LLORAN****" + "\n");
	System.out.println("¿Que podemos hacer por ti?"+ "\n" + "1. Comprar auto" + "\n" + "2. Quejas sobre algun vendedor" + "\n" + "3. Salir");
	System.out.print("Elija una opcion: ");

	//Tomamos opcion del usuario
	int opcion = sc.nextInt();

	//Un bolleano cerrar que sirve para cerrar los swtich
	boolean cerrar = true;

	//mientras while = true, no se va a cerrar el programa 
	while(cerrar == true){
	    switch(opcion){
	    case 1:
		System.out.println("Bienvenido a la compra de autos" + "\n" + "Estas son las opciones disponibles:");
		System.out.println("Opcion 1. Chevrolet Aveo 2022 $229,200 (color a elegir)");
		System.out.println("Opcion 2. Chevrolet Groove 2022 $332,900 (disponible solo en color rojo)");
		System.out.println("Opcion 3. Chevrolet Volt EUV 2020 $930,900 (puede ser electrico)");
		System.out.println("Opcion 3. Salir");
		System.out.print("Seleccione una opción: ");
		int opcion2 = sc.nextInt();

		boolean cerrar2 = true;
		while(cerrar2 == true){
		    switch(opcion2){
		    case 1:
			System.out.println("¿De que color lo quiere?");
			System.out.println("Opcion 1. rojo" + "\n" + "Opcion 2. blanco");
			System.out.print("Eliga una opcion: ");
			String color = sc.nextLine();
			if(color == "1"){
			    Auto autoAveo = new Auto(color);
			    
			System.out.println("Usted ha realizado la compra del siguiente auto: " +"\n" + autoAveo);
			System.out.println("Gracias por comprar en Los Chicos que Lloran");
			
			}else if(color == "2"){
			    Auto autoAveo = new Auto(color);
			    
			System.out.println("Usted ha realizado la compra del siguiente auto: " +"\n" + autoAveo);
			System.out.println("Gracias por comprar en Los Chicos que Lloran");
			}
			cerrar2 = false;
			break;
		    case 2:
			System.out.println("Usted ha seleccionado Chevrolet Groove 2022 $332,900");
			System.out.print("¿Esta segur@ que quiere adquirir este auto?");
			break;
		    }//fin del segundo switch
		}//fin de while(cerrar2)
		break;
		
	    case 2:
		break;

	    case 3:
		break;
	    

	    }//fin del switch
	}//fin del while 
	
    }//fin del metodo main
    
}//fin de la clase Agencia