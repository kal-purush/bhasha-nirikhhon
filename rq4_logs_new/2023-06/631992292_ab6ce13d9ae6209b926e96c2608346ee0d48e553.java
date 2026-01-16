package Model;

import Controller.Controller;

import java.util.ArrayList;
import java.util.List;

public class Ejercicios {

    static Controller controlador = new Controller();

    public static ArrayList<ArrayList<String>> Get_ejercicios() {

        // arraylists donde se van a almacenar los ejercicios
        ArrayList<String> pecho = new ArrayList<>(List.of("Press superior con Mancuernas", "Press Plano con Barra", "Cruces en Polea Baja", "Press Declinado", "Cruces en Polea Alta", "Aperturas"));
        ArrayList<String> espalda = new ArrayList<>(List.of("Jalon al pecho", "Remo en maquina", "Remo con mancuernas", "Remo en Punta", "Remo en Polea Baja", "Trapecios con mancuernas"));
        ArrayList<String> pierna = new ArrayList<>(List.of("Sentadilla", "Sentadilla Bulgara", "Prensa", "Femoral de Pie", "Femoral Tumbado", "Patada de glúteo en polea"));
        ArrayList<String> brazo = new ArrayList<>(List.of("Curl Biceps Mancuernas", "Curl en Maquina", "Press Frances en Polea", " Flexiones Apoyo Cerrado", "Press con Barra", "Curl martillo"));

        for (int i = 0; i < 2; i++) {
            int aleatorio = (int) Math.floor(Math.random() * ((pecho.size() - 1) - 0 + 1) + 0);
            pecho.remove(aleatorio);
            espalda.remove(aleatorio);
            pierna.remove(aleatorio);
            brazo.remove(aleatorio);
        }

        //Añadimos los ejercicios elegidos a la matriz resultante
        ArrayList<ArrayList<String>> matriz_ejercicios = new ArrayList<>();
        matriz_ejercicios.add(pecho);
        matriz_ejercicios.add(espalda);
        matriz_ejercicios.add(pierna);
        matriz_ejercicios.add(brazo);

        return matriz_ejercicios;
    }

    public static ArrayList<String> Get_tips_rutina() {

        ArrayList<String> tips = new ArrayList<>();

        if (controlador.getCliente_sesion_actual().getTipo_dieta().equals("Mantenimiento")) {
            String tip1 = "-Prioriza la técnica del ejercicio a la cantidad de peso que puedas tirar pero intentando progresar cada 1-2 semanas en esta última.";
            tips.add(tip1);
            String tip2 = "-Salvo en los ultimos 2 ejercicios no vas a ir al fallo. Vas a tirar mas o menos un 70-80% del maximo que podrias.";
            tips.add(tip2);
            String tip3 = "-Busca que los descansos entre series sean de 2-3 minutos. Dependiendo de tu nivel de fatiga.";
            tips.add(tip3);
            String tip4 = "-Vas a hacer 4 series da cada ejercicio.";
            tips.add(tip4);
        } else if (controlador.getCliente_sesion_actual().getTipo_dieta().equals("Deficit calórico (perder peso)")) {
            String tip1 = "-Prioriza la técnica del ejercicio a la cantidad de peso que puedas tirar. Si te ves capaz sube los pesos, pero no es nuestra prioridad.";
            tips.add(tip1);
            String tip2 = "-Vas a ir al fallo en cada ejercicio pero priorizando un numero de repeticiones alto y no menor del indicado (siempre con un peso medianamente exigente.)";
            tips.add(tip2);
            String tip3 = "-Busca que los descansos entre series sean de 1-1'5 o como mucho 2 minutos.";
            tips.add(tip3);
            String tip4 = "-Vas a hacer 5 series da cada ejercicio.";
            tips.add(tip4);
        } else if (controlador.getCliente_sesion_actual().getTipo_dieta().equals("Superhabit calórico (ganar peso)")) {
            String tip1 = "-Prioriza la técnica del ejercicio a la cantidad de peso que puedas tirar. Una vez que tengas dominado el ejercicio con cierto peso busca progresar.";
            tips.add(tip1);
            String tip2 = "-Vas a ir al fallo en cada ejercicio pero priorizando subir las cargas de peso. Estarás mas cerca de tirar menos repeticiones de las inidcadas por defecto que de tirar más. ";
            tips.add(tip2);
            String tip3 = "-Busca que los descansos entre series sean de 3-4 o hasta 5 minutos. Buscamos que estes 100% preparado para cada serie, por lo que la recuperación es esencial.";
            tips.add(tip3);
            String tip4 = "-Vas a hacer 4-5 series da cada ejercicio.";
            tips.add(tip4);
        }
        return tips;
    }
}