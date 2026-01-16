package arrays;

/**
 *
 * @author HysokA
 */
import java.util.Scanner;

public class Arrays {

    public static void inicializar(int a[]) {
        Scanner teclado = new Scanner(System.in);
        int tamaño, x;
        tamaño = a.length;
        for (x = 0; x < a.length; x++) {
            System.out.println("Dime el numero;");
            a[x] = teclado.nextInt();
        }

    }  //FIN INICIALIZARVV

    public static void imprimir(int a[]) {

        int talla, x;
        talla = a.length;
        for (x = 0; x < a.length-1; x++) {
            System.out.print(a[x]);
            System.out.printf(", ");
        }
        
        System.out.println(a[x]+".");
    }//FIN IMPRIMIRVV

    public static int maximo(int a[]) {

        int talla, x, maximo = 0;
        talla = a.length;
        for (x = 0; x < a.length; x++) {
            if (a[x] > maximo) {
                maximo = a[x];

            }
        }
        return maximo;
    }//FIN MAXIMOVV

    public static int minimo(int a[]) {

        int talla, x, minimo = 999;
        talla = a.length;
        for (x = 0; x < a.length; x++) {
            if (a[x] < minimo) {
                minimo = a[x];

            }
        }
        return minimo;

    }//FIN MINIMOVV

    public static boolean iguales(int a[], int a2[]) {

        int talla, x;
        boolean iguales = true;
        talla = a.length;
        for (x = 0; x < a.length; x++) {
            if (a[x] != a2[x]) {
                iguales = false;
            }
        }
        return iguales;
    }//FIN IGUALESVV

    public static boolean polindromo(int a[]) {

        int talla, x, mitad;

        boolean polindromo = true;
        talla = a.length;
        int i = talla - 1;
        mitad = talla / 2;
        for (x = 0; x < mitad; x++) {
            if (a[x] != a[i]) {
                polindromo = false;
                break;
            }

        }
        return polindromo;
    }//FIN POLINDROMOVV

    public static void ordenar(int a[]) {

        int talla, x, i, varaux;
        talla = a.length;
        for (i = 1; i < talla; i++) {
            for (x = 0; x < talla - i; x++) {
                if (a[x] > a[x + 1]) {
                    varaux = a[x + 1];
                    a[x + 1] = a[x];
                    a[x] = varaux;
                }//FIN IF
            }
        }//FIN PRIMER FOR
    }//FIN ORDENARVV

    public static double media(int a[]) {
        Scanner teclado = new Scanner(System.in);
        int talla, x;
        double media = 0, total = 0;
        talla = a.length;
        for (x = 0; x < a.length; x++) {
            total = total + (double) a[x];
        }
        media = (double) total / (double) talla;
        return media;
    }//FIN MEDIAVV

    public static int[] copiar(int a[], int v[]) {

        int talla, x;
        talla = a.length;
        //int v[]=new int[a.length];
        for (x = 0; x < a.length; x++) {
            v[x] = a[x];
        }

        return v;
    }//FIN MEDIAVV

    public static void main(String[] args) {
        Scanner teclado = new Scanner(System.in);
        Arrays llamar = new Arrays();
        int t, t2, op = 1, varaux;

        int array[];
        System.out.println("Tamaño del array?");
        t = teclado.nextInt();
        array = new int[t];

        while (op != 10) {
            do {
                System.out.println("Menu");
                System.out.println("1-Inicializar");
                System.out.println("2-Imprimir");
                System.out.println("3-Maximo");
                System.out.println("4-Minimo");
                System.out.println("5-Iguales");
                System.out.println("6-Polindromo");
                System.out.println("7-Ordenar");
                System.out.println("8-Media");
                System.out.println("9-Copiar");
                System.out.println("10-Salir");
                op = teclado.nextInt();
            } while (op > 10 || op < 1);

            switch (op) {

                case 1:
                    llamar.inicializar(array);
                    break;
                case 2:
                    llamar.imprimir(array);
                    break;
                case 3:
                    varaux = llamar.maximo(array);
                    System.out.println("maximo es: " + varaux);
                    break;
                case 4:
                    varaux = llamar.minimo(array);
                    System.out.println("minimo es : " + varaux);
                    break;
                case 5:
                    System.out.println("Tamaño del 2 array?");
                    t2 = teclado.nextInt();
                    int a2[] = new int[t2];
                    llamar.inicializar(a2);
                     if(llamar.iguales(array, a2)==true){
                        System.out.println("Si son iguales");
                                                    }
                    else {
                       System.out.println("No son iguales"); 
                    }
                    break;
                case 6:
                        if(llamar.polindromo(array)==true){
                        System.out.println("Si es polindromo");
                                                    }
                    else {
                       System.out.println("No es polindromo"); 
                    }
                    break;
                case 7:
                    llamar.ordenar(array);
                    llamar.imprimir(array);
                    break;
                case 8:
                    System.out.println("La media es: "+llamar.media(array));
                    break;
                case 9:

                    int[] v = new int[t];
                    llamar.copiar(array, v);
                    llamar.imprimir(v);
                    break;
                case 10:
                    break;

            }
System.out.println(" ");
        }//FIN WHILE MENUUUUUU

    }//FIN MAIN

}//FIN CLASS
