package ru.crypro;

/**
 * Created by lameroot on 29.06.16.
 */
public class TestCrypto {

    public static void main(String[] args) {
//        System.out.println(Math.floorMod(343, 11));
//        int i = 343%11;
//        System.out.println(i);
//        System.out.println(Math.pow(7,3));
//        System.out.println(generate(7,3,11));
        System.out.println((int)(Math.pow(7123,3))%1234567);

        for (int j = 0; j < 1000; j++) {
            //System.out.println(generate(70,j,11));

        }
    }

    public static int generate(int y, int x, int p) {
        return (int)Math.pow(y,x)%p;
    }
}