package fp.prestamos;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import fp.common.Area;
import fp.common.Persona;

public class FactoriaPrestamos {
    
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("dd-MM-yy");
    
    public static Prestamos leerPrestamos(String ruta) {
    	Prestamos res = new Prestamos();
    	try  {
    		List<String> lineas = Files.readAllLines(Paths.get(ruta));
    		for (String linea: lineas.subList(1, lineas.size())) {
    			res.añadirPrestamo(parsearPrestamo(linea));
    		}
    	} catch(Exception e) {
    		e.printStackTrace();
    	}
    	return res;
    }
    
    public static Prestamo parsearPrestamo(String linea) {
        String[] campos = linea.split(";");
        
        String prestamoId = campos[0].trim();
        Persona persona = new Persona(campos[1].trim(), campos[2].trim(), campos[3].trim());
        Integer dependientes = Integer.parseInt(campos[4].trim());
        Boolean graduado = campos[5].trim()=="Graduate";
        Boolean autonomo = parseaAutonomo(campos[6].trim());
        Float ingApl = Float.parseFloat(campos[7].trim());
        Float ingCoapl = Float.parseFloat(campos[8].trim());
        Integer cantidadPrestamos = Integer.parseInt(campos[9].trim());
        Integer plazo = Integer.parseInt(campos[10].trim());
        Boolean histCred = Boolean.parseBoolean(campos[11].trim());
        Area area = Area.valueOf(campos[12].toUpperCase().trim());
        LocalDate fechaPres = LocalDate.parse(campos[13].trim(), DATE_FORMATTER);
        List<Integer> listaActivos = new ArrayList<>();
        if (!campos[14].isEmpty()) {
            String[] activos = campos[14].split(",");
            for (String activo : activos) {
                listaActivos.add(Integer.parseInt(activo.trim()));
            }
        }
        
        return new Prestamo(prestamoId, persona, dependientes, graduado, autonomo, ingApl, ingCoapl, cantidadPrestamos, plazo, histCred, area, fechaPres, listaActivos);
    }
    
	private static Boolean parseaAutonomo(String cadena) {
		Boolean res = null;
		cadena = cadena.toUpperCase();
		if (cadena.equals("YES")) {
			res = true;
		}else {
			res = false;
		}
		return res;
	}
}