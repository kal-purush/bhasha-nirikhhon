
//usando sobrecarga de metodos, crear un metodo que reciba por parametro una cadena o un entero
//e indique de cuantos elementos esta compuesto, NOTA NO USAR CICLOS NI BUCLES

public class ContarElementos {

	public static void main(String[] args) {
		
		String palabra = "Hola mundo";
		int numero = 123;
		
		System.out.println("Longitud de "+palabra+" es: "+ contarElementos(palabra));
		System.out.println("Longitud de "+numero+" es: "+ contarElementos(numero));

	}
	
	
	public static int contarElementos(int numero) {
		String elementos = String.valueOf(numero);
		
		return elementos.length();
	}
		

	
	public static int contarElementos(String cadena) {
		return cadena.length();
	}

}