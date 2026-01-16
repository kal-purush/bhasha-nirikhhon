package shildt.book.exampleServiceClass;

import java.util.BitSet;

/**
 * Created by luchkovsky on 28.02.2016.
 */
public class BitSetDemo {
    public static void main(String[] args) {
        BitSet bits1 = new BitSet(16);
        BitSet bits2 = new BitSet(16);

        //Add some bits
        for (int i = 0; i < 16; i++){
            if ((i%2) == 0) bits1.set(i);
            if ((i%5) != 0) bits2.set(i);
        }

        System.out.println("Start maket in bits1: ");
        System.out.println(bits1);
        System.out.println("\nStart maket in bits2: ");
        System.out.println(bits2);

        //Logic AND to bits
        bits2.and(bits1);
        System.out.println("\nbits2 AND bits1: ");
        System.out.println(bits2);

        //Logic OR to bits
        bits2.or(bits1);
        System.out.println("\nbits2 OR bits1: ");
        System.out.println(bits2);

        //Logic XOR to bits
        bits2.xor(bits1);
        System.out.println("\nbits2 XOR bits1: ");
        System.out.println(bits2);

        System.out.println(bits1.size());
        System.out.println(bits2.hashCode());
    }
}