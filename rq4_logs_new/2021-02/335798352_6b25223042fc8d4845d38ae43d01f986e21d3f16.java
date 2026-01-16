package fit.core;

import java.util.HashMap;
import java.util.Map;

public class AttendanceHelper {

	public static Map<String, Boolean> random(int qtdRA, int perc) {
		Map<String, Boolean> lista = new HashMap<String, Boolean>();
		
		int bool = qtdRA * perc / 100;
		for(int z = 0; z < qtdRA; z++) {
			lista.put(String.valueOf(z), z < bool ? true: false);
		}
		
		return lista;
	}

}