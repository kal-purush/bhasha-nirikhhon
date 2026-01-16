/*
ID: nrziphe1
LANG: JAVA
TASK: combo
*/

import java.io.*;
import java.util.StringTokenizer;

class combo {
    static int n;
    public static int check(int x){
        if (x>n) return x-n;
        if (x<1) return x+n;
        return x;
    }

    public static int dist(int x){
        x=Math.abs(x);
        if ((n-x)<x) x=n-x;
        return x;
    }

    public static void main(String[] args) throws IOException {
        BufferedReader f = new BufferedReader(new FileReader("combo.in"));
        PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("combo.out")));
        StringTokenizer st = new StringTokenizer(f.readLine());
        n = Integer.parseInt(st.nextToken());
        int k=0;
        if (n<=5) {
            k=n*n*n;
            out.println(k);
            out.close();
            return;
        }
        k=5*5*5*2;
        st = new StringTokenizer(f.readLine());
        int x1 = Integer.parseInt(st.nextToken());
        int x2 = Integer.parseInt(st.nextToken());
        int x3 = Integer.parseInt(st.nextToken());
        st = new StringTokenizer(f.readLine());
        int y1 = Integer.parseInt(st.nextToken());
        int y2 = Integer.parseInt(st.nextToken());
        int y3 = Integer.parseInt(st.nextToken());
        for (int i1=-2;i1<=2;i1++)
            for (int i2=-2;i2<=2;i2++)
                for (int i3=-2;i3<=2;i3++){
                    int z1=check(x1+i1);
                    int z2=check(x2+i2);
                    int z3=check(x3+i3);
                    if (dist(z1-y1)<3&&dist(z2-y2)<3&&dist(z3-y3)<3){
                        k--;
                    }

                }
        out.println(k);
        out.close();
    }
}