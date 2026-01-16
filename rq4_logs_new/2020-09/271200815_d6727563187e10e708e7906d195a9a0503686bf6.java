package com.practice.hackerRank;

import java.io.*;
import java.util.ArrayList;

public class RabinKarpStringMatchingAlgorithm {
    static ArrayList<Integer> power1 = new ArrayList<>();
    static ArrayList<Integer> power2 = new ArrayList<>();
    static final int z = (int)1e9+7;
    static final int p1 = 59;
    static final int p2 = 79;
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        int test = Integer.parseInt(br.readLine());
        computePower(10000);
        while (test > 0){
            String s = br.readLine();
            String[] arr = s.split(" ");
            String a = arr[0];
            String b = arr[1];
            int count = stringMatch(a, b);
            bw.write(count + "");
            bw.newLine();
            test--;
        }
        bw.flush();
    }

    private static int stringMatch(String a, String b) {
        int count = 0;
        int ha1 = 0;
        int ha2 = 0;
        int hb1 = 0;
        int hb2 = 0;

        // compute ha1 and initial ha2
        for (int i = 0; i < a.length(); i++){
            ha1 = (ha1 + hashFunc1(a.charAt(i), i))%z;
            ha2 = (ha2 + hashFunc2(a.charAt(i), i))%z;

            hb1 = (hb1 + hashFunc1(b.charAt(i), i))%z;
            hb2 = (hb2 + hashFunc2(b.charAt(i), i))%z;
        }

        if (ha1 == hb1 && ha2 == hb2)
            count++;

        int k = a.length();

        for (int i = k; i < b.length(); i++){
            hb1 = (hb1 - hashFunc1(b.charAt(i-k), i-k) + z)%z;
            hb1 = (hb1 + hashFunc1(b.charAt(i), i))%z;
            hb2 = (hb2 - hashFunc2(b.charAt(i-k), i-k) + z)%z;
            hb2 = (hb2 + hashFunc2(b.charAt(i), i))%z;
            ha1 = (int)(((long)ha1*p1)%z);
            ha2 = (int)(((long)ha2*p2)%z);
            if (ha1 == hb1 && ha2 == hb2)
                count++;
        }

        return count;
    }

    private static int hashFunc1(char c, int i){
        long hash = ((long)c * power1.get(i))%z;
        return (int)hash;
    }

    private static int hashFunc2(char c, int i){
        long hash = ((long)c * power2.get(i))%z;
        return (int)hash;
    }

    private static void computePower(int n) {
        power1.add(p1);
        power2.add(p2);
        for (int i = 1; i <= n; i++){
            long mul1 = ((long)power1.get(i-1)*p1)%z;
            power1.add((int)mul1);
            long mul2 = ((long)power2.get(i-1)*p2)%z;
            power2.add((int)mul2);
        }
    }
}