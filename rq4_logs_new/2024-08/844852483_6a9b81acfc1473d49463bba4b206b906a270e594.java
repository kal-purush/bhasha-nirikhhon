package mx.unam.ciencias.modelado.practica1.files;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;

public class ReaderWriter {

    public static String[] read(String fileName)  {
        String line;
        LinkedList<String> l = null;
        BufferedReader in = null;

        try {
            in = new BufferedReader(new FileReader(fileName));
            l = new LinkedList<>();
            while ((line = in.readLine()) != null) {
                l.add(line);
            }
        } catch (FileNotFoundException e) {
            return null;//Esto se cambió para asegurar que simplemente no va a haber un manejo de un archivo que no existe
        } catch (IOException e) {
            System.out.println(e);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                System.out.println(e);
            }
        }

        return l.toArray(new String[l.size()]);
    }

    public static void write(String articulo, String fileName) {
        FileWriter out = null;

        try {
            out = new FileWriter(fileName, true);
            out.write(articulo);
            out.write("\n");
        } catch (IOException e) {
            System.out.println(e);
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException e) {
                System.out.println(e);
            }
        }
    }

}