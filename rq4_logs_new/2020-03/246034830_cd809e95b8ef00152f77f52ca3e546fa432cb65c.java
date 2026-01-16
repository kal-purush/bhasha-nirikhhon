import java.io.*;
import java.util.*;

public class Main {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        String ts = br.readLine(), ns;

        while ((ns = br.readLine()) != null) {
            int n = Integer.parseInt(ns);
            String[] arrA = br.readLine().trim().split(" ");
            String[] arrB = br.readLine().trim().split(" ");
            ArrayList<Integer> list = new ArrayList<>();
            Set<Integer> set = new HashSet<>();
            boolean found = false;
            boolean possible = true;

            for (int i = 0; i < n; i++) {
                int a = Integer.parseInt(arrA[i]);
                int b = Integer.parseInt(arrB[i]);
                int diff = b - a;

                if (diff < 0) {
                    possible = false;
                    break;
                }
                else if (diff == 0) {
                    if (found) {
                        list.add(diff);
                        set.add(diff);
                    }
                } else {
                    found = true;
                    list.add(diff);
                    set.add(diff);
                }
            }
            
            if (set.size() > 2) {
                possible = false;
            } else {
                if (set.size() == 2) {
                    int mx = Collections.max(list);
                    int mn = Collections.min(list);
                    if (mn == 0) {
                        int fIdx = list.indexOf(mx);
                        int lIdx = list.lastIndexOf(mx);
                        int altIdx = list.indexOf(mn);
                        if (altIdx > fIdx && altIdx < lIdx) {
                            possible = false;
                        }
                    } else {
                        possible = false;
                    }
                }
            }

            if (possible) System.out.println("YES");
            else System.out.println("NO");
        }
    }
}