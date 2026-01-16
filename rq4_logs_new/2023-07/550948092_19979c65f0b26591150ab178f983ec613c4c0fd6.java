package DoIt.NumberTheory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;

/*
    044 칵테일 만들기
    비율을 이용, 최대공약수, 최소공배수
 */
public class Solution {
    static ArrayList<cNode>[] A;
    static long lcm;
    static boolean visited[];
    static long D[]; // 각 노드 값 저장 배열
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int n = Integer.parseInt(br.readLine());
        StringTokenizer st;
        A =  new ArrayList[n];
        visited = new boolean[n];
        D = new long[n];
        lcm = 1;
        for(int i = 0; i < n; i++){
            A[i] = new ArrayList<cNode>();
        }
        for(int i = 0; i < n-1; i++) {
            st = new StringTokenizer(br.readLine());
            int a = Integer.parseInt(st.nextToken());
            int b = Integer.parseInt(st.nextToken());
            int p = Integer.parseInt(st.nextToken());
            int q = Integer.parseInt(st.nextToken());
            A[a].add(new cNode(b, p, q));
            A[b].add(new cNode(a, p, q));
            lcm *= (p * q / gcd(p, p)); // 최소공배수: 두 수의 곱을 최대 공약수로 나눈 것
        }
        D[0] = lcm;
        DFS(0);
        long mgcd = D[0];
        for(int i = 1; i < n; i++){
            mgcd = gcd(mgcd, D[i]);
        }
        for(int i = 0; i < n; i++){
            System.out.println(D[i] / mgcd + " ");
        }
    }

    public static long gcd(long a, long b){
        if(b == 0)
            return a;
        else
            return gcd(b, a % b);
    }

    public static void DFS(int Node){
        visited[Node] = true;
        for(cNode i: A[Node]){
            int next = i.getB();
            if(!visited[next]){
                D[next] = D[Node] * i.getQ() / i.getP();
                DFS(next);
            }
        }
    }
}
class cNode{
    int b;
    int p;
    int q;

    public cNode(int b, int p, int q) {
        super();
        this.b = b;
        this.p = p;
        this.q = q;
    }

    public int getB() {
        return b;
    }

    public int getP() {
        return p;
    }

    public int getQ() {
        return q;
    }
}