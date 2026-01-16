package tpe;

import model.Procesador;
import model.Solucion;
import model.Tarea;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class Backtracking {

    private Solucion mejorSolucion;
    private Servicios servicio;
    private int cantEstados;

    public Backtracking(Servicios s){
        this.servicio = s;
        cantEstados = 0;
    }
    /*
     * Esta solución genera un árbol con hasta P^T combinaciones, donde P es el número de procesadores y T es el número de tareas.
     *
     * En el primer nivel, tenemos P estados iniciales, en los cuales se asigna la primera tarea a cada procesador.
     * En el segundo nivel, para cada estado generado en el primer nivel, se recorre cada procesador y se asigna la segunda tarea.
     * Este proceso continúa de la misma forma hasta completar los T niveles, generando todas las combinaciones posibles de asignación.
     *
     * Además, se aplica una condición de poda basada en tareas críticas y en un tiempo máximo de ejecución para los procesadores refrigerados.
     */
    public Solucion backTracking(Integer tiempoMaxNoRefrigerados) {
        Solucion s = new Solucion();
        mejorSolucion = new Solucion();
        for (Procesador p : servicio.getProcesadores()) {
            s.getMapSolucion().put(p, new LinkedList<Tarea>());
            mejorSolucion.getMapSolucion().put(p, new LinkedList<Tarea>());
        }
        backTracking(s,servicio.getTareas(),tiempoMaxNoRefrigerados);
        return mejorSolucion;
    }

    private void backTracking(Solucion sParcial, List<Tarea> tRestantes, Integer tiempoMax) {
        cantEstados++;
        if (tRestantes.isEmpty()) {
            if (mejorSolucion.getTiempoFinal() ==0 || sParcial.getTiempoFinal() < mejorSolucion.getTiempoFinal()) {
                mejorSolucion = sParcial.clone();
            }
            return;
        }
        Tarea actual = tRestantes.get(0);

        for (Procesador p : sParcial.getMapSolucion().keySet()) {
            List<Tarea> tareasActuales = sParcial.getMapSolucion().get(p);
            Integer cantCriticas = 0, tiempoActual = 0;
            for (Tarea t : tareasActuales){
                cantCriticas+= t.getEsCritica() ? 1 : 0;
                tiempoActual+= t.getTiempoEjecucion();
            }
            if((!actual.getEsCritica() || cantCriticas <2)&&
                    (p.getEsRefrigerado() || tiempoActual+actual.getTiempoEjecucion()<=tiempoMax)){
                sParcial.getMapSolucion().get(p).add(actual);
                List<Tarea> tRestantesSiguiente = new ArrayList<>(tRestantes.subList(1, tRestantes.size()));
                backTracking(sParcial, tRestantesSiguiente,tiempoMax);
                sParcial.getMapSolucion().get(p).remove(actual);
            }

        }

    }

    public int getCantEstados(){
        return cantEstados;
    }

    public void imprimirSolucion(){
        System.out.println("BackTracking:");
        System.out.println("Solución obtenida: " + mejorSolucion);
        System.out.println("Solución obtenida tiempo máximo de ejecución: " + mejorSolucion.getTiempoFinal());
        System.out.println("Cantidad de estados generados: " + cantEstados);

    }
}