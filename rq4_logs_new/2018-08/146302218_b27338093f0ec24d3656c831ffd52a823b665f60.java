package javaapplication7;

import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JFrame;

class Graph {

    int W[][];
    int W2[][];
    boolean visited[] = new boolean[20];
    int distance[] = new int[20];
    int p[] = new int[20];
    int vertex, e;

    void creategraph() {
        int i, j, a, b, w;
        Scanner g = new Scanner(System.in);
        System.out.println("Enter number of vertices :");
        vertex = g.nextInt();
        W = new int[vertex + 1][vertex + 1];
        W2 = new int[vertex + 1][vertex + 1];
        System.out.println("Enter number of Edges :");
        e = g.nextInt();
        for (i = 1; i <= vertex; i++) {
            for (j = 1; j <= vertex; j++) {
                W[i][j] = 0;
                W2[i][j] = 0;
            }
        }

        for (i = 1; i <= vertex; i++) {
            p[i] = 0;
            visited[i] = false;
            distance[i] = Integer.MAX_VALUE;
        }
        for (i = 1; i <= e; i++) {
            System.out.print("\nEnter the number of first and second vertex and also the value its their edge");
            a = g.nextInt();
            b = g.nextInt();
            w = g.nextInt();
            W[a][b] = W[b][a] = w;
        }
    }

    void algoritm() {
        creategraph();// first we should creat a graph 
        int current, total, mincost, i;
        current = 1;
        distance[current] = 0;
        total = 1;
        visited[current] = true;
        while (total != vertex) {
            for (i = 1; i <= vertex; i++) {
                if (W[current][i] != 0) {
                    if (!visited[i]) {
                        if (distance[i] > W[current][i]) {
                            distance[i] = W[current][i];
                            p[i] = current;
                            W2[i][current] = W2[current][i] = W[current][i];
                        }
                    }
                }
            }
            mincost = Integer.MAX_VALUE;
            for (i = 1; i <= vertex; i++) {
                if (!visited[i]) {
                    if (distance[i] < mincost) {
                        mincost = distance[i];
                        current = i;
                    }
                }
            }
            visited[current] = true;
            total++;
        }

        mincost = 0;
        for (i = 1; i <= vertex; i++) {
            mincost = mincost + distance[i];

        }
        System.out.print("\n Minimum cost=" + mincost);
        System.out.print("\n Minimum Spanning tree is");

        for (i = 1; i <= vertex; i++) {
            System.out.print("\n vertex" + i + "is connected to" + p[i]);
        }
        show(W);
        show(W2);
        new Graphic(p, vertex, W);
    }

    private void show(int[][] W) {
        for (int i = 1; i < W.length; i++) {
            for (int j = 1; j < W.length; j++) {
                if (j == 1) {
                    System.out.println("");
                }
                System.out.print(" " + W[i][j]);
            }
        }
        System.out.println("");
        System.out.println("");
    }
}

public class Prime {

    public static void main(String args[]) {

        Graph g = new Graph();
        g.algoritm();
    }
}
/////////////////////

class Graphic extends JFrame {

    int[][] W;
    int x, late = 400;
    int[] p, locationx, locationy;
    Graph prim = new Graph();

    @Override
    public void paint(Graphics g) {
        super.paint(g);

        getlate(late);
        g.setColor(Color.red);
        g.fillRoundRect(350, 40, 40, 40, 50, 50);
        g.setColor(Color.green);
        getlate(1000);
        g.fillRoundRect(350, 40, 40, 40, 50, 50);
        g.setColor(Color.red);
        g.setFont(new Font("d", 2, 25));
        g.drawString("1", 359, 73);
        g.drawRoundRect(350, 40, 40, 40, 50, 50);
        locationx[1] = 365;
        locationy[1] = 78;

        for (int i = 2; i <= x; i++) {
            if (i % 2 != 0) {
                getlate(late);
                g.fillRoundRect(i * 180, 150 * i, 40, 40, 50, 50);
                g.setColor(Color.green);
                g.drawString(Integer.toString(i), i * 182, i * 160);
                g.setColor(Color.red);
                locationx[i] = i * 187 + 7;
                locationy[i] = i * 150;
            } else {
                getlate(late);
                g.fillRoundRect(i * 80, 150 * i, 40, 40, 50, 50);
                g.setColor(Color.green);
                g.drawString(Integer.toString(i), i * 85, i * 165);
                g.setColor(Color.red);
                locationx[i] = i * 87 + 7;
                locationy[i] = i * 150;
            }

        }
        for (int i = 1; i <= x; i++) {
            if (p[i] != 0) {
                getlate(late);
                g.setColor(Color.BLACK);
                g.drawLine(locationx[i], locationy[i], locationx[p[i]], locationy[p[i]]);
                g.drawString(Integer.toString(W[p[i]][i]), (locationx[i] + locationx[p[i]]) / 2 - 8,
                        (locationy[i] + locationy[p[i]]) / 2);
                System.out.print(" " + W[p[i]][i]);
            }
        }
    }

    public Graphic(int[] p, int x, int[][] W) {
        this.x = x;
        this.p = p;
        this.W = W;
        setVisible(true);
        setSize(700, 600);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);


        locationx = new int[x + 1];
        locationy = new int[x + 1];
    }

    private void getlate(int late) {
        try {
            TimeUnit.MILLISECONDS.sleep(late);
        } catch (InterruptedException ex) {
            Logger.getLogger(Graphic.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}