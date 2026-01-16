package testing;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class Ejercicio01LeerFichero {

	public static void main(String[] args) {
		
		File fichero = new File("/Users/hector/repos/quijote.rtf");
		
		try {
			FileReader fr = new FileReader(fichero);
			System.out.println("Fichero correcto!");
		} catch (FileNotFoundException e) {
			System.out.println();
			e.printStackTrace();
		}finally {
			System.out.println("Estamos en finally!");
		}
		System.out.println("Salida del try catch");
		
	}

}