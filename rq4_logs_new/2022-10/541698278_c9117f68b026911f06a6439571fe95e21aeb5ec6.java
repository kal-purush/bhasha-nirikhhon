package Ejercicios;

import Ejercicios.modelos.EstadoPartida;

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;

public class Ejercicio02_ObjetosBinario {

    private static Scanner sc;
    private static ArrayList<EstadoPartida> partidita;
    private static File partidaGuardada;


    static {
        sc = new Scanner(System.in);
        partidita = new ArrayList<>();
        partidaGuardada = new File("PartidaGuardada3.dat");
    }

    public static void main(String[] args) {

        int opcion;

        do {
            opcion= menu();
            switch (opcion) {
                case 1:
                    guardarPartida();
                    break;
                case 2:
                    mostrarPartidas();
                    break;
                case 3:
                    eliminarFichero();
                    break;
                case 4:
                    System.out.println("Hasta la próxima");
                    break;
                default:
                    System.out.println("Esa opción no existe. Hasta luegui");
            }
        }while(opcion!=4);

    }

    private static void eliminarFichero() {

        sc.nextLine();
        System.out.println("Esto eliminará el fichero, ¿estás seguro?");
        String respuesta= sc.nextLine();
        if(respuesta.equalsIgnoreCase("Si")){
            partidaGuardada.delete();
            partidita.clear();
            System.out.println("Fichero eliminado");
        }else{

        }

    }

    private static void mostrarPartidas() {
        FileInputStream fis;

        try {
            if (partidaGuardada.exists()) {


            fis = new FileInputStream(partidaGuardada);
            ObjectInputStream ois = new ObjectInputStream(fis);
            while (true) {
                EstadoPartida partida = (EstadoPartida) ois.readObject();
                System.out.printf("Tiene %d vida/s y está en la pantalla %d%n", partida.getVidasRestantes(), partida.getPantallaActual());
            }
        }else{
                System.out.println("No existe el fichero, crea una partida antes");
        }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (EOFException e) {

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    private static void guardarPartida() {
        FileOutputStream fos;
        EstadoPartida partida = crearPartida();
        partidita.add(partida);
        try {
            ObjectOutputStream ous;
            if (partidaGuardada.exists()) {
                ous = new MiObjectOutputStream(new FileOutputStream(partidaGuardada, true));

            } else {
                ous = new ObjectOutputStream(new FileOutputStream(partidaGuardada, true));
            }
            for (EstadoPartida p : partidita) {
                ous.writeObject(p);

            }

        } catch (IOException e) {

            throw new RuntimeException(e);

        }
    }

    private static EstadoPartida crearPartida() {
        int vida, pantalla;
        System.out.println("Dime la vida");
        vida = sc.nextInt();
        System.out.println("Dime la pantalla");
        pantalla = sc.nextInt();
        EstadoPartida partida = new EstadoPartida(vida, pantalla);
        return partida;
    }

    private static int menu() {
        System.out.println("1-Guardar Partida");
        System.out.println("2-Mostrar Partidas");
        System.out.println("3-Eliminar Fichero Partidas");
        System.out.println("4-Salir");
        return sc.nextInt();
    }
}