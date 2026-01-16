package ar.edu.um.biblioteca.Models;

import java.util.ArrayList;
import java.util.List;

public class GestionUsuarios {
    private List<Usuario> usuarios = new ArrayList<>();
    private SistemaPrestamos sistemaPrestamos;

    // Constructor
    public GestionUsuarios(SistemaPrestamos sistemaPrestamos) {
        this.sistemaPrestamos = sistemaPrestamos;
    }

    public void agregarUsuario(Usuario usuario) {
        Usuario nuevoUsuario = new Usuario(usuario.getNombre(), usuario.getApellido());
        usuarios.add(nuevoUsuario);
    }

    public Usuario buscarUsuarioPorNombreCompleto(String nombre, String apellido) {
        return usuarios.stream()
                .filter(usuario -> usuario.getNombre().equals(nombre) && usuario.getApellido().equals(apellido))
                .findFirst()
                .orElse(null);
    }

    public void registrarPrestamos(String nombre, String apellido, String isbn) {
        Usuario usuario = buscarUsuarioPorNombreCompleto(nombre, apellido);
        Prestamo prestamo = sistemaPrestamos.prestarLibro(isbn);
        // Lo agrega al historial del usuario
        usuario.getHistorialPrestamos().add(prestamo);

    }

}