package com.practice.spoj;

import java.io.*;
import java.util.ArrayList;

public class Prime1 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        int t = Integer.parseInt(br.readLine());
        boolean[] primes = new boolean[(int)1e5+1];
        ArrayList<Integer> list = computePrime(primes, (int)1e5);
        while (t > 0) {
            String s = br.readLine();
            String[] arr = s.split(" ");
            int a  = Integer.parseInt(arr[0]);
            int b = Integer.parseInt(arr[1]);
            ArrayList<Integer> ans = getPrimes(a, b, list, primes);
            for (int i : ans){
                bw.write(i + "");
                bw.newLine();
            }
            bw.newLine();
            t--;
        }
        bw.flush();
    }

    private static ArrayList<Integer> getPrimes(int a, int b, ArrayList<Integer> list, boolean[] primes) {
        ArrayList<Integer> list1 = new ArrayList<>();
        boolean[] prime = new boolean[b-a+1];
        if (a == 1)
            prime[0] = true;
        for (int i = 0; list.get(i) <= Math.sqrt(b); i++){
            int n = list.get(i);
            int idx = findFirstMultipleOfPrime(n, a, b, primes) - a;
            while (idx < prime.length){
                prime[idx] = true;
                idx += n;
            }
        }
        for (int i = 0; i < prime.length; i++)
        {
            if (!prime[i])
                list1.add(i+a);
        }
        return list1;
    }

    private static int findFirstMultipleOfPrime(int n, int a, int b, boolean[] primes) {
        for (int i = a; i<=b; i++)
        {
            if (i != n && i % n == 0)
                return i;
        }
        return 0;
    }

    private static ArrayList<Integer> computePrime(boolean[] primes, int n) {
        ArrayList<Integer> list = new ArrayList<>((int)1e4);
        primes[0] = true;
        primes[1] = true;
        for (int i = 2; i <= Math.sqrt(n); i++){
            if (!primes[i]){
                for (int j = i*i; j <= n; j+=i)
                    primes[j] = true;
            }
        }
        for (int i = 2; i <= n; i++){
            if (!primes[i])
                list.add(i);
        }
        return list;
    }
}