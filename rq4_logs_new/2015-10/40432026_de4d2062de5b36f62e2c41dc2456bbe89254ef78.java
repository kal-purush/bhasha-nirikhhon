import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;

public class Diff {

    int[][] p;
    ArrayList<String> text1;
    ArrayList<String> text2;
    boolean[] candidates1;
    boolean[] candidates2;

    public Diff() {
        this.text1 = new ArrayList<>();
        this.text2 = new ArrayList<>();
        this.p = new int[0][0];
    }

    public String findDiff(String a, String b) {
        for (int i = 0; i < a.length(); i++) {
            text1.add(String.valueOf(a.charAt(i)));
        }
        for (int j = 0; j < b.length(); j++) {
            text2.add(String.valueOf(b.charAt(j)));
        }
        boolean[][] k = new boolean[text1.size()][text2.size()];
        this.candidates1 = new boolean[text1.size()];
        this.candidates2 = new boolean[text2.size()];
        p = new int[text1.size() + 1][text2.size() + 1];

        for (int i = text1.size(); i >= 1; i--) {
            for (int j = text2.size(); j >= 1; j--) {
                if (text1.get(i - 1).equals(text2.get(j - 1)) && findP(i, j) > Math.max(findP(i - 1, j), findP(i, j - 1))) {
                    k[i - 1][j - 1] = true;
                }
            }
        }
        for (int i = 0; i < k.length; i++) {
            for (int j = 0; j < k[i].length; j++) {
                if (k[i][j]) {
                    candidates1[i] = true;
                    candidates2[j] = true;
                }
            }
        }
        StringBuilder res = new StringBuilder();
        res = res.append("--");
        for (int i = 0; i < candidates1.length; i++) {
            if (!candidates1[i]) {
                res.append(i+";");
            }
        }
        res = res.append("\n++");
        for (int j = 0; j < candidates2.length; j++) {
            if (!candidates2[j]) {
                res.append(j+";");
            }
        }
        return res.toString();
    }

    public int findP(int i, int j) {
        if (i == 0 || j == 0) {
            return 0;
        }
        if (p[i][j] == 0) {
            if (text1.get(i - 1).equals(text2.get(j - 1))) {
                p[i][j] = findP(i - 1, j - 1)+1;
            } else {
                p[i][j] = Math.max(findP(i - 1, j), findP(i, j - 1));
            }
        }
        return p[i][j];
    }
}