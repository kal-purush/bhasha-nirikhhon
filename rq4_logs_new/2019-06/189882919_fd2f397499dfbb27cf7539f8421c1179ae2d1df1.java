package co.com.ivancho;

import java.util.ArrayList;

import co.com.ivancho.jdbc.JDBCFacade;

/**
 * Clase inicial, recibe n par�metros y una consulta con la misma cantidad de
 * par�metros en forma de s�mbolos de interrogaci�n, luego ejecuta la consulta y
 * la devuelve como una cadena de texto.
 * 
 * @author IPinilla
 * @since 01/06/2018
 */
public class Main {

	/**
	 * M�todo inicial.
	 * 
	 * @param args Par�metros de entrada, no se usan :P
	 */
	public static void main(String[] args) {

		ArrayList<Object> parameters = new ArrayList<Object>();
		parameters.add("a");
		//parameters.add("ivan.pinilla@gmail.com");

		JDBCFacade facade = new JDBCFacade("postgresql");
		facade.getSelect("select * from userscore where username like '%?%'", parameters);
	}

}