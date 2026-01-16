/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package muxsubsum;

/**
 *
 * @author taha
 */
public class MuxSubSum {

    /**
     * @param args the command line arguments
     */
    static private int altKumeIndexBaslangıcı = 0;
    static private int altKumeIndexSonu = -1;

    public static void main(String[] args) {
        // TODO code application logic here
        int a[] = {4, -6, 5, 15, -42, -1, 2, -2};
        int maxSum;

        maxSum = maxSubSum1(a);

        System.out.println("maxSum = " + maxSum);
        for (int s = altKumeIndexBaslangıcı; s <= altKumeIndexSonu; s++) {
            System.out.print("a[" + s + "] ");
        }
    }

    public static int maxSubSum1(int[] a) {
        int maxSum = 0;

        for (int i = 0; i < a.length; i++) {
            for (int j = i; j < a.length; j++) {
                int thisSum = 0;

                for (int k = i; k <= j; k++) {
                    thisSum += a[k];
                }

                if (thisSum > maxSum) {
                    maxSum = thisSum;
                    altKumeIndexBaslangıcı = i;
                    altKumeIndexSonu = j;
                }
            }
        }
        return maxSum;
    }

}