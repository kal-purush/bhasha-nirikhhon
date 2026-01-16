import java.util.ArrayList;
import java.util.List;

public class Liga {
    private String nombre;
    private List<String> equipos;

    public Liga(String nombre) {
        this.nombre = nombre;
        this.equipos = new ArrayList<>();
    }

    public String getNombre() {
        return nombre;
    }

    public void agregarEquipo(String equipo) {
        equipos.add(equipo);
    }

    public void mostrarEquipos() {
        if (equipos.isEmpty()) {
            System.out.println("No hay equipos en esta liga.");
        } else {
            for (String equipo : equipos) {
                System.out.println("- " + equipo);
            }
        }
    }
}

