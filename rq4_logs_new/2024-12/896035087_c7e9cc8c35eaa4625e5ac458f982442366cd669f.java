package es.santander.ascender.proyecto11;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Implementación de la interfaz IGestionEstudiantes para gestionar
 * estudiantes y sus calificaciones.
 */
public class GestionEstudiantes implements IGestionEstudiantes {

    private final Map<String, Integer> estudiantes;

    /**
     * Constructor de la clase. Inicializa la estructura de datos para almacenar los estudiantes.
     */
    public GestionEstudiantes() {
        this.estudiantes = new HashMap<>();
    }

    @Override
    public boolean agregarEstudiante(String nombre, int calificacion) {
        if (estudiantes.containsKey(nombre)) {
            return false; // El estudiante ya existe, no se puede agregar de nuevo
        }
        estudiantes.put(nombre, calificacion);
        return true;
    }

    @Override
    public Integer obtenerCalificacion(String nombre) {
        return estudiantes.get(nombre); // Devuelve la calificación o null si no existe
    }

    @Override
    public Map<String, Integer> obtenerEstudiantesYCalificaciones() {
        return new HashMap<>(estudiantes); // Devuelve una copia del mapa para evitar modificaciones externas
    }

    @Override
    public boolean existeEstudiante(String nombre) {
        return estudiantes.containsKey(nombre); // Verifica si el estudiante existe
    }

    @Override
    public boolean eliminarEstudiante(String nombre) {
        if (estudiantes.containsKey(nombre)) {
            estudiantes.remove(nombre); // Elimina el estudiante si existe
            return true;
        }
        return false;
    }

    @Override
    public void agregarEstudiantes(Set<String> nuevosEstudiantes, Map<String, Integer> nuevasCalificaciones) {
        for (String estudiante : nuevosEstudiantes) {
            if (nuevasCalificaciones.containsKey(estudiante)) {
                estudiantes.put(estudiante, nuevasCalificaciones.get(estudiante)); // Agrega o actualiza la calificación
            }
        }
    }
}