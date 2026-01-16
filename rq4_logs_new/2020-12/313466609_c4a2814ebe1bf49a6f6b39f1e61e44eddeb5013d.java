/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ProblemaCaballo;

/**
 *
 * @author Gabriel
 */
public class ProblemaCaballo {
    
    static int[][] tablero;
    public int x;//valor del tamaño en el eje vertical
    public int y;//valor del tamaño en el eje horizontal

    public ProblemaCaballo(int x, int y) {
        tablero = new int[x][y]; //declaramos el tamaño del tablero de tamaño x*y
        this.x = x;
        this.y = y;
        //llenamos el tablero de cero
        for (int i = 0; i < x; i++) {
            for (int j = 0; j < y; j++) {
                tablero[i][j] = 0;
            }
        }

    }

    boolean valorValido(int vert, int hor) { //validador de si los valores estan dentro de la tabla
        if (vert < 0 || vert > x - 1 || hor < 0 || hor > y - 1) {  //
            return false;
        }
        if (tablero[vert][hor] != 0) {
            return false;
        }
        return true;
    }

    public boolean rutaDelProblemaCaballo(int vert, int hor, int num) {//metodo que llama los demás metodos y llena el tablero

        if (valorValido(vert, hor)) {
            tablero[vert][hor] = num;//si la inicionado inicial en las cordenadas vert,hor esta dentro del tablero entonces toma el valor de num 
        }
        int[][] posibles = obtSaltosPosibles(vert, hor);//sacamos los posibles saltos que dara el ProblemaProblemaCaballo
        ordenarSaltos(posibles);//ordenamos los posibles saltos dependiendo de los posibles saltos que tengan.              
        for (int i = 0; i < posibles.length; i++) {
            if (rutaDelProblemaCaballo(posibles[i][0], posibles[i][1], num + 1)) {
                return true; //a partir de ya haber sacado los saltos del ProblemaProblemaCaballo  haremos un tipo de recursividad que genere las siguientes nibilidades para rellenar el tablero
            }
        }
        if (num == x * y) {
            return true; // Si el num toma el valor de x*y este ya recorrio toda el tablero y se termina
        }
        return false;
    }

    int[][] obtSaltosPosibles(int vert, int hor) {
        //se sacaran los diversos saltos a partir de la forma en la que nos enseño el profesor de la clases de analisis de Algoritmos
        /*
           |   |   |   |   |   |   |   |
           |   |   |   |   |   |   |   | 
           |   |   | 3 |   | 4 |   |   |
           |   | 2 |   |   |   | 5 |   |   
           |   |   |   | 1 |   |   |   |
           |   | 9 |   |   |   | 6 |   |
           |   |   | 8 |   | 7 |   |   |
           |   |   |   |   |   |   |   |
           |   |   |   |   |   |   |   |
        
         */
        int[][] valor = new int[8][2]; //guardamos los Saltos en una matriz de 8 por 2 para guardar a donde saltara a partir de la ubicacion  
        int n = 0;
        if (valorValido(vert - 1, hor - 2)) {
            valor[n][0] = vert - 1;
            valor[n++][1] = hor - 2;
        }
        if (valorValido(vert - 2, hor - 1)) {
            valor[n][0] = vert - 2;
            valor[n++][1] = hor - 1;
        }
        if (valorValido(vert - 2, hor + 1)) {
            valor[n][0] = vert - 2;
            valor[n++][1] = hor + 1;
        }

        if (valorValido(vert - 1, hor + 2)) {
            valor[n][0] = vert - 1;
            valor[n++][1] = hor + 2;
        }
        if (valorValido(vert + 1, hor + 2)) {
            valor[n][0] = vert + 1;
            valor[n++][1] = hor + 2;
        }
        if (valorValido(vert + 2, hor + 1)) {
            valor[n][0] = vert + 2;
            valor[n++][1] = hor + 1;
        }
        if (valorValido(vert + 2, hor - 1)) {
            valor[n][0] = vert + 2;
            valor[n++][1] = hor - 1;
        }

        if (valorValido(vert + 1, hor - 2)) {
            valor[n][0] = vert + 1;
            valor[n++][1] = hor - 2;
        }

        int[][] aux = new int[n][2]; //declaramos un auxiliar de tamaño n * 2 que nos ayude a guardarlos
        for (int i = 0; i < n; i++) { //guardamos con un for
            aux[i][0] = valor[i][0];
            aux[i][1] = valor[i][1];
        }
        return aux; //returnamos el aux
    }

    void ordenarSaltos(int[][] posibles) { //ordenaremos los saltos dependiendo de la cantidad posibles Saltos que tiene de menor a mayor
        for (int i = 0; i < posibles.length; i++) {
            for (int j = i + 1; j < posibles.length; j++) {
                int cantSaltosI = obtSaltosPosibles(posibles[i][0], posibles[i][1]).length; //Sacamos la cantidad de posibles saltos 
                int cantSaltosj = obtSaltosPosibles(posibles[j][0], posibles[j][1]).length; //Sacamos la cantidad de posibles saltos de la posicion i+1 
                if (cantSaltosj < cantSaltosI) { //comparamos una posicion con la otra y si la posicion i+1 tiene menor cantidad que i entonces cambiamos la posicion a la derecha  
                    int aux0 = posibles[i][0];
                    int aux1 = posibles[i][1];
                    posibles[i][1] = posibles[j][1];
                    posibles[j][1] = aux1;
                    posibles[i][0] = posibles[j][0];
                    posibles[j][0] = aux0;
                  
                }
            }
        }
    }

    public void mostrarTablero() {//mostramos el tablero 
        for (int i = 0; i < x; i++) {
            for (int j = 0; j < y; j++) {

                System.out.printf("  %2d ", tablero[i][j]);//es para mostrar en 2d
            }

            System.out.println("");
        }
    }

    public static void main(String[] args) {
         int x=2;
         int y=10;
        for (int i = 0; i < x; i++) {
            for (int j = 0; j < y; j++) {
                ProblemaCaballo cab = new ProblemaCaballo(x, y);// vamos al constructor
                cab.rutaDelProblemaCaballo(i, j, 1);//ejecutara el metodo   
                cab.mostrarTablero();//llamamos el metodo para mostrar el tablero
                System.out.println("\n");
            }

        }
    }
}