package org.example.leetcode.indexoffirstoccurrenceinstring;

import java.util.concurrent.ExecutionException;

/**
 * https://leetcode.com/problems/find-the-index-of-the-first-occurrence-in-a-string/description/
 */
public class Executor {

    private static void hello() {
        synchronized (Executor.class) {

        }

    }

    public static class MyClass {
        volatile int counter;
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
//        int position = strStrMySolution("footballer", "ball");
        int position = strStrMySolution("mississippi", "issip");
        System.out.println(position);
    }

    public static int strStrMySolution(String haystack, String needle) {
        byte[] needleBytes = needle.getBytes();
        byte[] haystackBytes = haystack.getBytes();

        if (needleBytes.length > haystackBytes.length) {
            return -1;
        }

        for (int i = 0; i < haystackBytes.length; i++) {
            label:
            if (haystackBytes[i] == needleBytes[0]) {
                if (needleBytes.length > haystackBytes.length - i) {
                    return -1;
                }
                for (int k = 0; k < needleBytes.length; k++) {
                    if (needleBytes[k] != haystackBytes[i + k]) {
                        break label;
                    }
                }
                return i;
            }
        }

        return -1;
    }

}