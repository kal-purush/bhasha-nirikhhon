package tec.poo.practica;
import java.util.*;

public class Biblioteca
{
    private List<Libro> inventario;

    public List<Libro> getInventario() {
        return inventario;
    }

    public Biblioteca() {
        this.inventario = new ArrayList<>();
    }

    public void agregarLibro(String titulo, String autor, String isbn) {

        Libro libro = new Libro();
        libro.setTitulo(titulo);
        libro.setAutor(autor);
        libro.setIsbn(isbn);

        this.inventario.add(libro);

    }

    public boolean buscarLibro(String prompt, int tipo){
        boolean existe = false;

        if (tipo == 0){
            for (int i = 0; i < inventario.size(); i++ ){
                if (inventario.get(i).getTitulo() == prompt ){
                    existe = true;
                    break;
                }
            }
        }
        else if (tipo == 1) {
            for (int i = 0; i < inventario.size(); i++ ){
                if (inventario.get(i).getAutor() == prompt ){
                    existe = true;
                    break;
                }
            }
        }
        return existe;

    }

}