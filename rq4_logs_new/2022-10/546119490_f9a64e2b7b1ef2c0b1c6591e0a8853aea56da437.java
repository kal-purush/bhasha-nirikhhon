package Hanoi;
import java.util.*;

/**
 * ハノイの塔の解説 - 初期設定、出力
 */
class Main {
    /*
    piles: 3本の杭をlistに格納
    名前なし: 杭は、LinkedList、円盤を格納
    名前なし: 円盤の大きさを整数で表す
    */

    static List<LinkedList<Integer>> piles;

    static void initialize(int n) {
        piles = new LinkedList<>();

        //3本の杭を作る
        for(int i=0; i<3; i++)
            piles.add(new LinkedList<>());

        for(int i=n; i>=1; i--)
            piles.get(0).add(i);
    }

    static void printPiles() {

        System.out.println("--");

        for(int i=0; i<3; i++){
            System.out.println(i + ":");

            for(int disk: piles.get(i))
                System.out.println(" " + disk);

            System.out.println();
        }
    }

    public static void main(String args[] ) {
        int n = 3;
        System.out.println(n);

        initialize(n);
        printPiles() ;
    }
}