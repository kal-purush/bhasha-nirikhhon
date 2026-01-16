package principal;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Consultar implements interfazBase {

    private List<String> al = new ArrayList<String>();
    private Inicializar ini;
    private String nombre;
    private String codigo;
    private boolean existe = false;

    @Override
    public void operacion() {
        BufferedReader Lectura;
        ini = new Inicializar();
        ini.operacion();
        try {
            Lectura = new BufferedReader(new FileReader(ini.getFile()));
            String leerLinea = Lectura.readLine();

            while (leerLinea != null) {
                this.al.add(leerLinea);                            
                leerLinea = Lectura.readLine();
            }
            int i = al.size() - 1;

            while (i != -1) {
                try {
                    String linea = al.get(i);
                    String[] parts = linea.split("/");
                    String cod = parts[0];
                    String nom = parts[1];

                    if (cod.equals(this.codigo) && nom.equals(this.nombre)) {
                        this.existe = true;
                    }
                } catch (ArrayIndexOutOfBoundsException w) {

                }
                i -= 1;

            }

        } catch (FileNotFoundException ex) {
            Logger.getLogger(Retirar.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Retirar.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public boolean existeUser() {
        return existe;
    }

    public void setCod(String cod) {
        this.codigo = cod;
    }

    public void setNom(String nom) {
        this.nombre = nom;
    }

}