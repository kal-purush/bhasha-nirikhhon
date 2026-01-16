package com.company;

import java.io.*;
import java.math.BigInteger;
import java.util.*;

public class Solution {

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        InputStream inputStream = System.in;
        OutputStream outputStream = System.out;
        InputReader in = new InputReader(inputStream);
        PrintWriter out = new PrintWriter(outputStream);
        int v = in.nextInt();
        int ans = 10000000;
        ArrayList<Integer> vFacts = factors(v);
        for(Integer n: vFacts) {

            int rest = v/n;
            ArrayList<Integer> vOverNFacts = factors(rest);
            for(Integer m: vOverNFacts) {

                int k = rest/m;
//                out.print(n + " " + m + " " + k);
//                out.println();
                ans = Math.min(ans, n * m + n * k + m * k);
            }
//            out.println();
        }
        out.println(2 * ans);
        out.close();
    }
    public static ArrayList<Integer> factors(int x) {
        ArrayList<Integer> facts = new ArrayList<>();
        for(int i=1; i * i <= x; i++) {
            if (x % i == 0) {
                facts.add(i);
                if (i != x/i) facts.add(x/i);
            }
        }
        return facts;
    }

    static class InputReader {
        public BufferedReader reader;
        public StringTokenizer tokenizer;

        public InputReader(InputStream stream) {
            reader = new BufferedReader(new InputStreamReader(stream), 32768);
            tokenizer = null;
        }

        public String next() {
            while (tokenizer == null || !tokenizer.hasMoreTokens()) {
                try {
                    tokenizer = new StringTokenizer(reader.readLine());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return tokenizer.nextToken();
        }

        public int nextInt() {
            return Integer.parseInt(next());
        }

    }

}