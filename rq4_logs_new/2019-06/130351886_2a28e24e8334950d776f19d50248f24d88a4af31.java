package e_04;

import java.util.Scanner;

/*
 * 演習4-18：1からnまでの整数値の2乗値を表示するプログラムを作成せよ。
 * 作成者：岐部 佳織
 * 作成日：2019年6月24日
 */
public class e_04_18 {

    public static void main(String[] args) {

        //正の整数であるかを判定する値。
        final int THRESHOLD_VALUE = 1;
        //計算する最初の値。
        final int START_VALUE = 1;

        //キーボードからの入力を受け取る用意をする。
        Scanner inKeyboard = new Scanner(System.in);
        //入力値を受け取る用意をする。
        int inputSquare;
        //入力値が正の整数であるかを判定する。
        do {
            //タイトルを表示し入力を促す。
            System.out.print("nの値：");
            //入力された値を取得する。
            inputSquare = inKeyboard.nextInt();
            //入力値が1未満の場合
            if (inputSquare < THRESHOLD_VALUE) {
                //正の整数を入力するようアナウンスする。
                System.out.println("1以上の整数値を入力して下さい。");
            }
            //入力値が1未満の場合は再度タイトルを表示する。
        } while (inputSquare < THRESHOLD_VALUE);

        //2乗される値が入力値に到達するまで計算を繰り返す。
        for (int squareCalculation = START_VALUE; squareCalculation <= inputSquare; squareCalculation++) {
            //2乗される値が入力値に到達するまで2乗値を表示する。
            System.out.println(squareCalculation + "の2乗は" + (squareCalculation * squareCalculation));
        }
    }
}