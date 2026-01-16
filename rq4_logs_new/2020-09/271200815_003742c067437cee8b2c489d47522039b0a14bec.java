package com.practice.hackerRank;

import java.io.*;
import java.util.ArrayList;

public class PrimeSum {
    static ArrayList<Integer> list = new ArrayList<>((int)1e6+1);
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        int test = Integer.parseInt(br.readLine());
        boolean[] primes = getPrimes((int)1e6);
        countPrimes((int)1e6, primes);
        while (test > 0){
            int m = Integer.parseInt(br.readLine());
            int ans = list.get(m);
            bw.write(ans + "");
            bw.newLine();
            test--;
        }
        bw.flush();
    }

    private static void countPrimes(int n, boolean[] primes) {
        list.add(0);
        list.add(0);
        for (int i = 2; i <= n; i++){
            if (!primes[i] && !valid(i, primes))
                list.add(list.get(i-1) + 1);
            else
                list.add(list.get(i-1));
        }
    }

    private static boolean valid(int i, boolean[] primes) {
        int length = (int)Math.log10(i)+1;
        int j = i;
        while (j > 0){
            if (j%10 == 0)
                return true;
            j /= 10;
        }
        int m = 10;
        while (length-- > 0){
            if (primes[i%m])
                return true;
            m *= 10;
        }
        return false;
    }

    private static boolean[] getPrimes(int n) {
        boolean[] primes = new boolean[n+1];
        primes[0] = true;
        primes[1] = true;
        int i;
        for (i = 2; i <= Math.sqrt(n); i++){
            if (!primes[i]){
                for (int j = i * i; j <= n; j += i)
                    primes[j] = true;
            }
        }
        return primes;
    }
}