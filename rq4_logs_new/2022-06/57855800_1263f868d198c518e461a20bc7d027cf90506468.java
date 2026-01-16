package com.github.lucaseo90.algo.days21.day6;

import java.util.LinkedList;
import java.util.Queue;

public class NumberOfProvinces {
    public int findCircleNum(int[][] isConnected) {
        boolean[][] visited = new boolean[isConnected.length][];
        for(int i = 0;i < isConnected.length;i++){
            visited[i] = new boolean[isConnected.length];
        }
        return findCircle(isConnected, visited);
    }

    private int findCircle(int[][] isConnected, boolean[][] visited) {
        int circle = 0;
        for (int i = 0;i < isConnected.length; i++) {
            if(!visited[i][i]) {
                Queue<Integer> queue = new LinkedList<>();
                queue.add(i);
                visited[i][i] = true;

                while(!queue.isEmpty()) {
                    int vertex = queue.poll();

                    // forward
                    for(int edge = vertex + 1;edge < isConnected.length; edge++){
                        if(isConnected[vertex][edge] == 1 && !visited[edge][edge]) {
                            visited[edge][edge] = true;
                            queue.add(edge);
                        }
                    }

                    // backward
                    for(int edge = vertex - 1;edge >= 0; edge--){
                        if(isConnected[vertex][edge] == 1 && !visited[edge][edge]) {
                            visited[edge][edge] = true;
                            queue.add(edge);
                        }
                    }
                }
                circle++;
            }
        }
        return circle;
    }
}