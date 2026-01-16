package practice.codeforces;

import java.util.*;
import java.util.LinkedList;

/**
 * @Link https://codeforces.com/problemset/problem/2/A
 * status : ac
 */
public class A_Winner {
    static class NameScore {
        public Integer score;
        public String name;
    }

    public static void main(String[] args) {
        Map<String, Integer> a = new HashMap<String, Integer>();
        Map<String, Integer> b = new HashMap<String, Integer>();
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        String name = null;
        Integer score = 0;
        int version = 1;
        Integer maxScore = -100000000;
        NameScore[] nameScores = new NameScore[1050];
        int i=0;
        while (i < n) {
            name = sc.next();
            score = sc.nextInt();
            nameScores[i] = new NameScore();
            nameScores[i].name = name;
            nameScores[i].score = score;
            if (a.get(name) == null) {
                a.put(name, 0);
            }
            a.put(name, a.get(name) + score);
            i++;
        }
        int max = 0;
        for (i=0; i<n; i++) {
            if (a.get(nameScores[i].name) > max)
                max = a.get(nameScores[i].name);
        }
        for (i=0; i<n; i++) {
            if (b.get(nameScores[i].name) == null)
                b.put(nameScores[i].name, 0);
            b.put(nameScores[i].name, nameScores[i].score + b.get(nameScores[i].name));
            if (b.get(nameScores[i].name)>=max && a.get(nameScores[i].name)>=max) {
                System.out.println(nameScores[i].name);
                break;
            }
        }
    }
}