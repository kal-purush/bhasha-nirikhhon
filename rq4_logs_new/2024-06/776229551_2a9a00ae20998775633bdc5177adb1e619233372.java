package main.base;

import java.util.ArrayList;

public class RSA {
    public static final int START=200;
    private int p=11;
    private int q=7;
    private int n;
    private int phiN;
    private int e;
    private int d;
    public void makePQ() {
        p=(int)(Math.random()*START);
        q=(int)(Math.random()*START);
        while(p==q || p<100 || q<100 || (Java200Math2.isPrime(p)) || (Java200Math2.isPrime(q))) {
            p=(int)(Math.random()*START);
            q=(int)(Math.random()*START);
        }
    }
    public void makeN() {
        n=p*q;
    }
    public int getN() {
        return n;
    }
    public void print() {
        System.out.printf("(p,q)=(%d,%d)\n"
        + "(n,phi)=(%d,%d)\n(e,d)=(%d,%d)\n",p,q,n,phiN,e,d);
    }
    public void makePHIN() {
        this.phiN=(p-1)*(q-1);
    }
    public int getPhiN() {
        return phiN;
    }
    public void makeE() {
        ArrayList<Integer> es = new ArrayList<>();
        for (int i = 2; i < phiN; i++) {
            if (Java200Math3.gcd(phiN, i)==1) {
                es.add(i);
//                System.out.printf(i+",");
            }
        }
        e=es.get(es.size()-1); // 가장 큰 것(랜덤으로 만들면 좋다)
    }
    public void makeD() {
        int count=2;
        while (!(count*e%phiN==1 && count!=e)) {
            count++;
        }
        d=count;
    }
    public int toCiph(int m) {
        int tot=1;
        for (int i = 0; i < e; i++) {
            tot=(tot*m)%n;
        }
        return tot;
    }
    public int toUnCiph(int c) {
        int tot=1;
        for (int i = 0; i < d; i++) {
            tot=(tot*c)%n;
        }
        return tot;
    }
    public static void main(String[] args) {
        RSA aa = new RSA();
//        aa.makePQ();
        aa.makeN();
        aa.makePHIN();
        aa.makeE();
        aa.makeD();
        aa.print();
        for (int i = 65; i < 200; i++) {
            int bbb=aa.toCiph(i);
            int ccc=aa.toUnCiph(bbb);
            System.out.println(String.format("%d, %d, %d",i,bbb,ccc));
        }
    }
}