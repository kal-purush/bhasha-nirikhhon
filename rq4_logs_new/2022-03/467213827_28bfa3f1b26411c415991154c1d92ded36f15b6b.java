package md.tekwill.topic3;

public class Operations {
    static void printArithmeticOperations(int x, int y){
        System.out.println("Adunarea: " + (x + y));
        System.out.println("Scaderea: " + (x - y));
        System.out.println("Inmultirea: " + (x * y));
        System.out.println("Impartirea: " + (x / y));
        System.out.println("Restul impartirii: " + (x % y));
        System.out.println("Pre-Increment x: " + (++x));
        System.out.println("Post-Increment y: " + (y++));
        System.out.println("Pre-Decrement x: " + (--x));
        System.out.println("Post-Decrement y: " + (y--));

    };
    static void printLogicOperations(boolean x, boolean y){
        System.out.println("xANDy = " + (x&&y));
        System.out.println("xORy = " + (x||y));
        System.out.println("xNOTy = " + !(x&&y));
    };
    static void printRelationalOperations(int x, int y){
        System.out.println("Egalitate:" + (x == y));
        System.out.println("Diferenta:" + (x != y));
    };
    static void useForAndWhile(int limita){
        int index = 5;

        while (index <= limita) {
            System.out.println(index + "");
        index++;
        }

        for (int x = 5; x <= limita; x++) {
            System.out.println(x + " ");
        }
        System.out.println("Ciclul for s-a executat ");
    };

}