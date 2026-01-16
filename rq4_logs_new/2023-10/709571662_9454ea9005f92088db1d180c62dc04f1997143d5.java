import modelo.Equipo;
import modelo.Partido;
import modelo.Pronostico;
import modelo.ResultadoEnum;
import modelo.Ronda;
import controlador.PartidoController;
import controlador.RondaController;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        // Crear instancias de Equipos
        Equipo equipo1 = new Equipo();
        equipo1.setNombre("Equipo 1");

        Equipo equipo2 = new Equipo();
        equipo2.setNombre("Equipo 2");

        // Crear instancias de Partido
        Partido partido1 = new Partido(equipo1, equipo2, 2, 1);
        Partido partido2 = new Partido(equipo2, equipo1, 1, 1);

        // Crear una lista de partidos para una Ronda
        List<Partido> partidosDeRonda = new ArrayList<>();
        partidosDeRonda.add(partido1);
        partidosDeRonda.add(partido2);

        // Crear una instancia de Ronda con los partidos
        Ronda ronda = new Ronda("1", partidosDeRonda);

        // Crear instancias de Pronostico relacionadas a los partidos
        Pronostico pronostico1 = new Pronostico(equipo1, equipo2, ResultadoEnum.ganador);
        Pronostico pronostico2 = new Pronostico(equipo2, equipo1, ResultadoEnum.empate);

        // Crear una lista de pronósticos para un Partido
        List<Pronostico> pronosticosDePartido = new ArrayList<>();
        pronosticosDePartido.add(pronostico1);
        pronosticosDePartido.add(pronostico2);

        // Crear una instancia de PartidoController con los partidos y pronósticos
        PartidoController partidoController = new PartidoController(partidosDeRonda.toArray(new Partido[0]), pronosticosDePartido.toArray(new Pronostico[0]));

        // Calcular puntajes y mostrar resultados
        partidoController.calcularPuntajes();
        partidoController.mostrarResultados();

        // Crear una instancia de RondaController con la ronda
        RondaController rondaController = new RondaController(new Ronda[]{ronda});

        // Calcular puntajes acumulativos y mostrar resultados de la ronda
        rondaController.calcularPuntajesAcumulativos();
        rondaController.mostrarResultadosRonda();
    }
}