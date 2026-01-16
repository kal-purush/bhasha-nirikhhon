package com.practice.hackerRank;

import java.io.*;

public class KMPStringMatchingAlgorithm {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        int test = Integer.parseInt(br.readLine());
        while (test > 0){
            String[] arr = br.readLine().split(" ");
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
        int[] table = computeTable(a);
        int count = 0;
        int i = 0;
        int j = 0;
        while (j < b.length()){
            if (a.charAt(i) == b.charAt(j)){
                i++;
                j++;
            }
            else if (i > 0)
                i = table[i-1];
            else
                j++;

            if (i == a.length())
            {
                count++;
                i = table[i-1];
            }
        }
        return count;
    }

    private static int[] computeTable(String a) {
        int[] table = new int[a.length()];
        int i = 0;
        int j = 1;
        while (j < a.length()){
            if (a.charAt(i) == a.charAt(j)){
                table[j] = i+1;
                i++;
                j++;
            }
            else if (i > 0){
                table[j] = table[i-1];
                j++;
            }
            else
                j++;
        }
        return table;
    }
}