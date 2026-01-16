package Proyecto;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.GregorianCalendar;
import java.util.Set;

//Clase donde vamos a implementar pequeños metodos que pueden usarse desde las diferentes clases del proyecto
public class Util {

 
 //Recibe un String fecha, lo subdivide en dia, mes, año y devuelve un GregorianCalendar
 public static GregorianCalendar PasarAGregorianCalendar(String s){
  
  String auxCalendar[] = s.trim().split("/");
  int dia = Integer.parseInt(auxCalendar[0].trim());
  int mes = Integer.parseInt(auxCalendar[1].trim());
  int anho = Integer.parseInt(auxCalendar[2].trim());
  GregorianCalendar c = new GregorianCalendar();
  c.set(anho, mes, dia);
  
  return c;
 }

  
 
 //METODO PARA ESCRIBIR LOS MAPAS ALUMNO, PROFESOR Y ASIGNATURA A FICHEROS .TXT
/* public static void MapaAFichero(String nomFich) throws IOException {
 
  BufferedWriter output = new BufferedWriter(new FileWriter(nomFich));
  Set<String> keys = Proyecto.mapAlumnos.keySet();
  
 
  
 }
 */
 
}