package david;

import java.util.Scanner;

public class Main {
    static void print(String text) {
        System.out.print(text);
    }
    static void println(String text) {
        System.out.println(text);
    }
    static char scan_char() {
        Scanner in = new Scanner(System.in);
        String s = in.nextLine();
        if(s.length() == 0)
            return '\n';
        char res = s.charAt(0);
        return res;
    }
    static int scan_int() {
        Scanner in = new Scanner(System.in);
        int s = in.nextInt();
        return s;
    }
    static final int ROWS = 8;
    static final int COLS = 10;

    static void printPlayGround(char[][] playground) {
        // vor allem
        println("nun kommen die DaTeN");
        for (int y = 0; y < ROWS; y++) {
            // vor der Datenzeile
            print("|  ");

            for (int x = 0; x < COLS; x++) {
                print("" + playground[y][x]);

                // Zwischen den Daten
                print("  ");
            }

            // nach der Datenzeile
            print("|");
            print("\n");    //Absatz
        }

        // nach allem

        int unten =0;
        while (unten <= COLS){
            print("___");
            unten++;
        }
        print("\n ");   // Absatz + Leerzeichen
        for(int zahl=0; zahl <COLS; zahl++){
            print("  "+zahl);
        }
//        for(int unten =0; unten<=COLS; unten++) {
////            print("____");
////        }
        println("");    // Absatz am Ende
    }


    public static void main(String[] args) {

        char[][] playground = new char[ROWS][COLS];
        for(int y=0; y<ROWS; y++) {
            for(int x=0; x<COLS; x++) {
                int tmp = x + y;             //Beispieldaten werdden erzeugt
                if(tmp%3==0) {
                    playground[y][x] = ' ';
                } else if(tmp%3==1) {
                    playground[y][x] = 'X';
                } else {
                    playground[y][x] = 'O';
                }
            }
        }

        printPlayGround(playground);
        println("Wählen Sie eine Feldnummer (0-9):");
        int column = get_valid_num();

        //erste Eingabe speichern
        playground [0][column] = 'E';
        printPlayGround(playground);
    }

    // Methoden



    static int get_valid_num() {
        // 1. scan_char aufrufen

        // 2. Wert überpfüfen, muss zwischen 0-9 liegen
        int get_zahl;
        do {
            get_zahl = scan_int();
            println("Deine Eingabe ist " + get_zahl + ". Liegt die Zahl außerhalb des Geltungsbereich, erscheint erneut die Eingabeaufforderung");
        }
        while (get_zahl >= -1 && get_zahl >= 10);

        //      Bei Fehler, Fehlerausgabe machen, so das der Spieler neu eingeben muss ( also zurück zu 1.)
        // 3. char in int umwandeln ( also '0' -> 0 und nicht ascii 65 oder was auch immer) (kann auch vor 2.)
        // 4. Ergebnis zurückgeben (return)
        return get_zahl;
    }




    static void beispiele() {

        // write your code here

        //int[][] matrix = {{1, 2, 3}, {4, 5, 6}};
        char[][] playground = new char[COLS][ROWS]; //Erstellung der Matrix -->Koordinatensystem ist obenlinks!!!
        for(int y=0; y<playground.length; y++) {  //Anzahl der "Schranksysteme"
            for(int x=0; x<playground[y].length; x++) { //Anzahl der Fächer des Schrankelements an der Stelle y
                int tmp = x + y;             //Beispieldaten werdden erzeugt
                if(tmp%3==0) {
                    playground[y][x] = '+';
                } else if(tmp%3==1) {
                    playground[y][x] = 'X';
                } else {
                    playground[y][x] = 'O';
                }
            }
        }

        printPlayGround(playground);  //Funktionsaufruf


        println("   :" + playground[2][9]);  //Ausgeben des Wertes aus Zeile 3 Spalte 9

        for(int y=0; y<playground.length; y++) {  //jede Zeile wird durchgelaufen

            char[] zeile = playground[y];  // Schrank Y wird "Zeile" zugewiessen (Wichtig, nicht die Schublade und keine Kopie!!!)

            for(int x=0; x<zeile.length; x++) { //Es wird jede Schublade x des Schrankes y durchlaufen
                print(""+zeile[x]+ "  ");  //Ausgabe Inhald der Schublade x
            }
            println("");
        }
    }
}
 /* while(true) {
            println("Was ist dein Liebligsbuchstabe oder Ziffer?");
            char liebling = scan_char();
            println("Aso, also " + liebling);
            if (liebling == ' ') {
                println("Spinner :D");
            }
        }
  */