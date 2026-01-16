package tec.poo.tareas;
import java.util.List;

public class GestorTareas 
{
    // solicitud: Define una clase GestorTareas que contenga una instancia de TareasRepository con los metodos
    private TareasRepository repository;

    public GestorTareas(TareasRepository repository) 
    {
        this.repository = repository;
    }

    public void agregarTarea(String tarea) 
    {
        List<String> tareas = repository.cargarTareas();
        tareas.add(tarea);
        repository.guardarTareas(tareas);
    }

    public void eliminarTarea(String tarea) 
    {
        List<String> tareas = repository.cargarTareas();
        tareas.remove(tarea);
        repository.guardarTareas(tareas);
    }

    public List<String> cargarTareasDesdeArchivo() 
    {
        return repository.cargarTareas();
    }

}