package boletin_2;

public class cadenaUtil {

		public static String invertirCaracteresPorPares(String original) {
			
			StringBuilder sb = new StringBuilder();
			
			if(original !=null && !original.isEmpty()) {//Si tiene contenido invierto la cadena
				for (int i =0; i<original.length()-1; i+=2) {
					sb.append(original.charAt(i+1)).append(original.charAt(i));
				}
				sb.append(original.length()%2==1?original.charAt(original.length()-1):"");
			}
					
			return sb.toString();

	}

}