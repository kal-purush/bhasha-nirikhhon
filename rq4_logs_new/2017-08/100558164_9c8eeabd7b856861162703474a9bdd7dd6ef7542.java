/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package practica1_201503741;

import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 * @author Eddy Alejandro
 */
public class Jugar_201503741 {

    public Jugar_201503741() {
    }
    int inicial = 0;
    String[][] matriz1;
    String[][] matriz2;
    int randomX;
    int randomY;
    int x;
    int y;
    double pos;
    String nivel = "";

    public void crear(int n) {
        inicial = n;

        matriz1 = new String[n][n];
        matriz2 = new String[n][n];

        //matriz del juego
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                matriz1[i][j] = "X";
            }
        }
        //imprimiendo
        
        if (n == 4) {
            nivel = "PRINCIPIANTE";
        } else if (n == 6) {
            nivel = "INTERMEDIO";
        } else if (n == 8) {
            nivel = "EXPERTO";
        }
        System.out.println("--------------------------------------------------");
        System.out.println("              NIVEL " + nivel);
        for (int i = 0; i < matriz1.length; i++) {
            System.out.println("");
            System.out.print("              ");
            for (int j = 0; j < matriz1[i].length; j++) {
                System.out.print("|" + matriz1[i][j] + "|");
            }
        }

        matriz2(n);

        System.out.println("");
        System.out.println("               -------------------");
        System.out.println("                  Voltear: V");
        System.out.println("                  Reiniciar: R");
        System.out.println("                  Salir: S");
        System.out.println("                Ingrese Opciton:");
        //ingresando option para jugar
        Scanner Joption = new Scanner(System.in);
        String letra;
        letra = Joption.nextLine();
        System.out.println("               -------------------");
        Option(letra);
    }

    void Option(String letra) {
        switch (letra) {
            case "V":
                voltear();
                break;
            case "v":
                voltear();
                break;
            case "R":
                crear(inicial);
                break;
            case "r":
                crear(inicial);
                break;
            case "S":
                System.out.println("salir");
                break;
            case "s":
                System.out.println("salir");
                break;
        }
    }

    private void matriz2(int n) {
        //matriz de minas y numeros
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                matriz2[i][j] = " ";
            }
        }

        //determinando el numero de minas segun el nivel del juego
        int nMinas = 0;
        switch (n) {
            case 4:
                nMinas = 4;
                break;
            case 6:
                nMinas = 8;
                break;
            case 8:
                nMinas = 12;
                break;
        }
        //llenando la matriz2 con el numero de minas
        for (int i = 0; i < nMinas; i++) {
            int verificador = 0;
            while (verificador == 0) {
//                System.out.println("nMinas: " + nMinas);
                randomX = ThreadLocalRandom.current().nextInt(0, n - 1);
                randomY = ThreadLocalRandom.current().nextInt(0, n - 1);
//                System.out.println("randomX: " + randomX + "randomY: " + randomY);
                if (matriz2[randomX][randomY] == " ") {
                    matriz2[randomX][randomY] = "*";
                    verificador = 1;
                }
            }
        }
        //llenando de numeros las matriz 2
        for (int i = 0; i < matriz2.length; i++) {
            for (int j = 0; j < matriz2.length; j++) {
                if (matriz2[i][j] == " ") {
                    int numero = 0;
                    String num;
                    //una posicion arriba
                    try {
                        if (matriz2[i - 1][j] == "*") {
                            numero = numero + 1;
                        }
                    } catch (Exception e) {
                    }
                    //una posicion abajo
                    try {
                        if (matriz2[i + 1][j] == "*") {
                            numero = numero + 1;
                        }
                    } catch (Exception e) {
                    }
                    //una posicion a la izquierda
                    try {
                        if (matriz2[i][j - 1] == "*") {
                            numero = numero + 1;
                        }
                    } catch (Exception e) {
                    }
                    //una posicion a la derecha
                    try {
                        if (matriz2[i][j + 1] == "*") {
                            numero = numero + 1;
                        }
                    } catch (Exception e) {
                    }
                    //esquina superior izquierda
                    try {
                        if (matriz2[i - 1][j - 1] == "*") {
                            numero = numero + 1;
                        }
                    } catch (Exception e) {
                    }
                    //esquina superior derecha
                    try {
                        if (matriz2[i - 1][j + 1] == "*") {
                            numero = numero + 1;
                        }
                    } catch (Exception e) {
                    }
                    //esquina inferior izquierda
                    try {
                        if (matriz2[i + 1][j - 1] == "*") {
                            numero = numero + 1;
                        }
                    } catch (Exception e) {
                    }
                    //esquina inferior derecha
                    try {
                        if (matriz2[i + 1][j + 1] == "*") {
                            numero = numero + 1;
                        }
                    } catch (Exception e) {
                    }
//                    System.out.println("numero: "+numero);
                    num = String.valueOf(numero);
                    matriz2[i][j] = num;
                }
            }
        }

        //imprimiendo matriz de minas y numero
        System.out.println("");
        for (int i = 0; i < matriz2.length; i++) {
            System.out.println("");
            System.out.print("              ");
            for (int j = 0; j < matriz2[i].length; j++) {
                System.out.print("|" + matriz2[i][j] + "|");
            }
        }

    }

    private void voltear() {
        //ingresando posiciones x,y
        Scanner ingresar = new Scanner(System.in);
        System.out.println("               Ingrese posicion:");
        pos = ingresar.nextDouble();
//        System.out.println("pos: " + pos);
        x = (int) (pos % 10);
//        System.out.println("x: " + x);
        y = (int) ((pos * 10) - (x * 10));
//        System.out.println("y: " + y);
        int posX = x - 1;
        int posY = y - 1;
        if (matriz2[posX][posY] != "*") {
            //en la posicion elegida
            matriz1[posX][posY] = matriz2[posX][posY];
            //una posicion arriba
            try {
                if (matriz2[posX - 1][posY] != "*") {
                    matriz1[posX - 1][posY] = matriz2[posX - 1][posY];
                }
            } catch (Exception e) {
            }
            //una posicion abajo
            try {
                if (matriz2[posX + 1][posY] != "*") {
                    matriz1[posX + 1][posY] = matriz2[posX + 1][posY];
                }
            } catch (Exception e) {
            }
            //una posicion a la izquierda
            try {
                if (matriz2[posX][posY - 1] != "*") {
                    matriz1[posX][posY - 1] = matriz2[posX][posY - 1];
                }
            } catch (Exception e) {
            }
            //una posicion a la derecha
            try {
                if (matriz2[posX][posY + 1] != "*") {
                    matriz1[posX][posY + 1] = matriz2[posX][posY + 1];
                }
            } catch (Exception e) {
            }
            seguir_jugando();
        }else if(matriz2[posX][posY] == "*"){
            derrota();
        }
        
    }

    private void seguir_jugando() {
        System.out.println("--------------------------------------------------");
        System.out.println("              NIVEL " + nivel);
        for (int i = 0; i < matriz1.length; i++) {
            System.out.println("");
            System.out.print("              ");
            for (int j = 0; j < matriz1[i].length; j++) {
                System.out.print("|" + matriz1[i][j] + "|");
            }
        }

        matriz2_seguir();

        System.out.println("");
        System.out.println("               -------------------");
        System.out.println("                  Voltear: V");
        System.out.println("                  Reiniciar: R");
        System.out.println("                  Salir: S");
        System.out.println("                Ingrese Opciton:");
        //ingresando option para jugar
        Scanner Joption = new Scanner(System.in);
        String letra;
        letra = Joption.nextLine();
        System.out.println("               -------------------");
        Option(letra);
    }

    private void matriz2_seguir() {
        //imprimiendo matriz de minas y numero
        System.out.println("");
        for (int i = 0; i < matriz2.length; i++) {
            System.out.println("");
            System.out.print("              ");
            for (int j = 0; j < matriz2[i].length; j++) {
                System.out.print("|" + matriz2[i][j] + "|");
            }
        }
    }

    private void derrota() {
        System.out.println("--------------------------------------------------");
        System.out.println("              NIVEL " + nivel);
        System.out.println("               PERDISTE!!!!");
        //copiando todos los elementos de la matriz2 a la matriz1
        for(int i=0;i<matriz2.length;i++){
            for(int j=0; j<matriz2.length; j++){
                matriz1[i][j]=matriz2[i][j];
            }
        }
        //imprimiendo matriz1
        for (int i = 0; i < matriz1.length; i++) {
            System.out.println("");
            System.out.print("              ");
            for (int j = 0; j < matriz1[i].length; j++) {
                System.out.print("|" + matriz1[i][j] + "|");
            }
        }
        //imprimiendo matriz de minas y numero
        System.out.println("");
        for (int i = 0; i < matriz2.length; i++) {
            System.out.println("");
            System.out.print("              ");
            for (int j = 0; j < matriz2[i].length; j++) {
                System.out.print("|" + matriz2[i][j] + "|");
            }
        }
        System.out.println("");
        System.out.println("      Presione ENTER para reiniciar el juego");
        Scanner leer = new Scanner(System.in);
        String r="";
        r=leer.nextLine();
        new Practica1_201503741().menu();
    }

}