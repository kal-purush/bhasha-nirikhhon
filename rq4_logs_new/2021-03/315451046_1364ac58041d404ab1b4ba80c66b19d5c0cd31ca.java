package com.example.pacmanlike.gamemap;

import android.widget.ArrayAdapter;

import com.example.pacmanlike.gamemap.tiles.Tile;
import com.example.pacmanlike.main.AppConstants;
import com.example.pacmanlike.objects.Direction;
import com.example.pacmanlike.objects.Vector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class BFS {

    private class Node{

        Vector position;
        Node parent;
        Direction direction;

        public Node(Vector position, Node parent, Direction direction){
            this.position = position;
            this.parent = parent;
            this.direction = direction;
        }
    }


    GameMap gameMap;

    public BFS(GameMap map){
        this.gameMap = map;
    }


    public boolean existsPath(Vector start, Vector target){
        List<Direction> tmp = findPath(start, target);

        if(tmp.size() > 0){
            return true;
        }

        return false;
    }


    public boolean achievedTarget(Vector target, Node node){

        if(target.x == node.position.x && target.y == node.position.y){
            return true;
        }

        return false;
    }

    public List<Direction> getPath(Node node) {
        List<Direction> tmp = new ArrayList<Direction>();

        while (node.parent != null){

            tmp.add(node.direction);
            node = node.parent;
        }

        return tmp;
    }


    public List<Direction> findPath(Vector start, Vector target){

        List<Direction> path = new ArrayList<Direction>();


        // Mark all the vertices as not visited
        boolean[][] visited = new boolean[AppConstants.MAP_SIZE_Y][AppConstants.MAP_SIZE_X];

        for(int y = 0; y< AppConstants.MAP_SIZE_Y; y++){
            for(int x = 0; x < AppConstants.MAP_SIZE_X; x++) {
                visited[y][x] = false;
            }
        }

        // Create a queue for BFS
        LinkedList<Node> queue = new LinkedList();


        visited[start.y][start.x] = true;
        queue.add(new Node(start, null, null));

        Node s;
        while (queue.size() != 0) {

            s = queue.poll();
            int x = s.position.x;
            int y = s.position.y;

            List<Direction> possibleMoves = gameMap.getTile(x, y).getPossibleMoves();
            for (Direction d: possibleMoves) {

                switch (d){
                    case LEFT:
                        if(!visited[y][x - 1]){
                            visited[y][x - 1] = true;
                            Node node = new Node(new Vector(x - 1, y), s, Direction.LEFT);
                            queue.add(node);

                            if(achievedTarget(target, node)){
                                path = getPath(node);
                            }
                        }
                        break;
                    case RIGHT:
                        if(!visited[y][x + 1]){
                            visited[y][x + 1] = true;
                            Node node = new Node(new Vector(x + 1, y), s, Direction.RIGHT);
                            queue.add(node);

                            if(achievedTarget(target, node)){
                                path = getPath(node);
                            }
                        }
                        break;
                    case DOWN:
                        if(!visited[y + 1][x]){
                            visited[y + 1][x] = true;
                            Node node = new Node(new Vector(x, y + 1), s, Direction.DOWN);
                            queue.add(node);

                            if(achievedTarget(target, node)){
                                path = getPath(node);
                            }

                        }
                        break;
                    case UP:
                        if(!visited[y - 1][x]){
                            visited[y - 1][x] = true;
                            Node node = new Node(new Vector(x, y - 1), s, Direction.UP);
                            queue.add(node);

                            if(achievedTarget(target, node)){
                                path = getPath(node);
                            }
                        }
                        break;
                    default:
                        break;
                }
            }
        }

        return  path;
    }
}