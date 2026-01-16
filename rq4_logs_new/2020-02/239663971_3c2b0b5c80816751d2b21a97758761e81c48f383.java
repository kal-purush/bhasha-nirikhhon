package pe.edu.ceid.simi.management.infraestructure;

import java.sql.Date;
import java.util.Map;

public class Tratamiento {

	public static int tratarEntero(String columna, Map<String, Object> row) {
		int ret = -1;
		
		try {
			ret = Integer.parseInt(row.get(columna).toString());
		} catch (Exception e) {}
		
		return ret;
	}

	public static String tratarString(String columna, Map<String, Object> row) {
		String ret = "";
		
		try {
			ret = row.get(columna).toString();
		} catch (Exception e) {}
		
		return ret;
	}

	public static Date tratarDate(String columna, Map<String, Object> row) {
		Date ret = null;
		
		try {
			ret = (Date) row.get(columna);
		} catch (Exception e) {}
		
		return ret;
	}

}