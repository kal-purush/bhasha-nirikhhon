/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.edu.poli.ingesoft2.granreto;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author fercris
 */
public class FachadaGranReto implements IFachadaGranReto {

    private List<String> lstFilasArchivo;
    Path pathRutaArchivoValida;

    public FachadaGranReto() {
        lstFilasArchivo = new ArrayList<>();
    }

    public boolean esNumero(String string) {
        if (string == null || string.isEmpty()) {
            return false;
        }
        int i = 0;
        if (string.charAt(0) == '-') {
            if (string.length() > 1) {
                i++;
            } else {
                return false;
            }
        }
        for (; i < string.length(); i++) {
            if (!Character.isDigit(string.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public int cantidadOcurrenciasCaracter(String palabra, char caracter) {
        int count = 0;
        for (int i = 0; i < palabra.length(); i++) {
            if (palabra.charAt(i) == caracter) {
                count++;
            }
        }
        return count;
    }

    public boolean esNumeroConComas(String string) {
        if (string == null || string.isEmpty()) {
            return false;
        }
        int i = 0;
        if (string.charAt(0) == '-') {
            if (string.length() > 1) {
                i++;
            } else {
                return false;
            }
        }
        //Validar si está en notacion cientifica

        //Validar si tiene decimales
        if (cantidadOcurrenciasCaracter(string, ',') > 1) {
            return false;
        }
        String numero = string.replace(",", "");
        for (; i < numero.length(); i++) {
            if (!Character.isDigit(numero.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public boolean esNotacionCientifica(String string) {
        if (string == null || string.isEmpty()) {
            return false;
        }
        int i = 0;
        if (string.charAt(0) == '-') {
            if (string.length() > 1) {
                i++;
            } else {
                return false;
            }
        }

        if ((cantidadOcurrenciasCaracter(string, ',') == 1 && cantidadOcurrenciasCaracter(string, 'E') == 1)
                && (cantidadOcurrenciasCaracter(string, '+') == 1) || cantidadOcurrenciasCaracter(string, '-') == 1) {
            return esNumero(string.replace("-", "").replace("+", "").replace("E", "").replace(",", ""));
        } else {
            return false;
        }
    }

    private boolean esArchivoValidoLectura(Path rutaArchivo) {

        return Files.exists(rutaArchivo, LinkOption.NOFOLLOW_LINKS) && Files.isReadable(rutaArchivo)
                && !Files.isDirectory(rutaArchivo, LinkOption.NOFOLLOW_LINKS) && Files.isRegularFile(rutaArchivo, LinkOption.NOFOLLOW_LINKS);
    }

    private void cargarFilasArchivo() {
        try {
            lstFilasArchivo.clear();
            List<String> lineasArchivo = Files.readAllLines(pathRutaArchivoValida);
            //Eliminar lineas vacías, espacios y tabuladores
            for (String lineaArchivo : lineasArchivo) {
                if (!lineaArchivo.trim().isEmpty()) {
                    lstFilasArchivo.add(lineaArchivo.replace(" ", "").replace("\t", ""));
                }
            }

            for (String filaArchivoLeida : lstFilasArchivo) {
                System.out.println(filaArchivoLeida);
            }

        } catch (IOException ex) {
            Logger.getLogger(FachadaGranReto.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void cargarArchivo(String rutaArchivo) throws GranRetoException {

        if (esArchivoValidoLectura(Paths.get(rutaArchivo))) {
            pathRutaArchivoValida = Paths.get(rutaArchivo);
            cargarFilasArchivo();

        } else {
            throw new GranRetoException("Error de entrada-salida", new Throwable("El archivo no existe, no es legible, es una carpeta o no es un archivo regular."));
        }

    }

    @Override
    public String calcular() {
        return "";

    }

}