package com.example.cohalz.entropy;

import android.app.Activity;
import android.graphics.Point;
import java.lang.System;
import java.util.LinkedList;

import static com.example.cohalz.entropy.Constants.*;

/**
 * Created by cohalz on 15/06/30.
 */
public class Board {
    private int[][] state;
    public ViewBoard viewBoard;
    private boolean isolateFlag;

    public Board(Activity a){
        state = new int[5][5];
        int[][] tmpArray = {
                {0,   0,  0,  0,  0},
                {0,  -1, -1, -1,  0},
                {-1, -1, -1, -1, -1},
                {1,  -1, -1, -1,  1},
                {1,   1,  1,  1,  1}
        };
        for(int i = 0; i < 5; i++){
            state[i] = tmpArray[i].clone();
        }
        this.viewBoard = new ViewBoard(state,a);
        this.isolateFlag = false;
    }

    public void move(Move m){
        int tmp = state[m.from.y][m.from.x];
        state[m.from.y][m.from.x] = state[m.to.y][m.to.x];
        state[m.to.y][m.to.x] = tmp;
        viewBoard.changeImage(m);
    }

    public boolean checkFromPoint(Player p, Point point){
        isolateFlag = false;
        if (state[point.y][point.x] == p.number &&
            !isIsolate(point,p.number) &&
                movable(point,p.number) > 0) {
                    isolateToBlank();
                    viewBoard.display(state);
                    return true;
        }
        return false;
    }

    public boolean checkToPoint(Point point){
        return state[point.y][point.x] == MOVABLE;
    }

    public void movableToBlank(){
        for(int y = 0; y < 5; y++){
            for(int x = 0; x < 5; x++){
                if(state[y][x] >= MOVABLE) state[y][x] = BLANK;
            }
        }
        viewBoard.display(state);
    }


    public int movable(Point p, int number) {
        int count = 0;
        if (isContainsAlone(number)) {
            createAloneFlag(number);
            isolateFlag = true;
        }
        count += countVec(p, 1, 1, number);
        count += countVec(p, 1, 0, number);
        count += countVec(p, 1, -1, number);
        count += countVec(p, 0, 1, number);
        count += countVec(p, 0, -1, number);
        count += countVec(p, -1, 1, number);
        count += countVec(p, -1, 0, number);
        count += countVec(p, -1, -1, number);
        return count;
    }
     public void createAloneFlag(int number) {
        LinkedList<Point> aloneList = new LinkedList<>();
        for (int y = 0; y < 5; y++) {
            for (int x = 0; x < 5; x++) {
                Point p = new Point(x, y);
                if (isAlone(p)) {
                    aloneList.add(p);
                    //最初は普通に色塗る
                    //二回目以降は色が塗ってあるところの数字を増やしてそれ以外はリセット
                    //数字を元に戻すのを繰り返す
                }
            }

        }
        for (int i = 0; i < aloneList.size(); i++) {
            Point p = aloneList.remove();
            if (i == 0) {
                changeAroundColor(p, -1, number + 3);

            } else {
                changeAroundColor(p, 3 + number, 5 + number);
                countDown(number);
            }
        }
    }

    public void isolateToBlank(){
        for (int y = 0; y < 5; y++) {
            for (int x = 0; x < 5; x++) {
                if(state[y][x] > MOVABLE) state[y][x] = BLANK;
            }
        }
    }

    public void countDown(int number) {
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                if (state[i][j] == 3 + number) state[i][j] = BLANK;
                if (state[i][j] == 3 + number + 2) state[i][j] -= 2;

            }
        }
    }

    public boolean isContainsAlone(int number) {
        for (int y = 0; y < 5; y++) {
            for (int x = 0; x < 5; x++) {
                if (isAlone(new Point(x,y)) && state[y][x] == number) return true;
            }
        }
        return false;
    }

    public boolean isAlone(Point p) {
        return isIsolate(p, P1) && isIsolate(p, P2);
    }

    public boolean isIsolate(Point p, int ban) {
        if (p.y > 0) {
            if (state[p.y - 1][p.x] == ban) return false;
            if (p.x > 0) {
                if (state[p.y - 1][p.x - 1] == ban) return false;
            }
            if (p.x < 4) {
                if (state[p.y - 1][p.x + 1] == ban) return false;
            }
        }
        if (p.y < 4) {
            if (state[p.y + 1][p.x] == ban) return false;
            if (p.x > 0) {
                if (state[p.y + 1][p.x - 1] == ban) return false;
            }
            if (p.x < 4) {
                if (state[p.y + 1][p.x + 1] == ban) return false;
            }
        }
        if (p.x > 0) {
            if (state[p.y][p.x - 1] == ban) return false;
        }
        if (p.x < 4) {
            if (state[p.y][p.x + 1] == ban) return false;
        }
        return true;
    }

    public int countVec(Point p, int xVec, int yVec, int ban) {
        int xi = p.x + xVec;
        int yi = p.y + yVec;
        int count = 0;
        while (xi < 5 && xi >= 0 && yi < 5 && yi >= 0 && state[yi][xi] != P1 && state[yi][xi] != P2) {
            if (this.isolateFlag) {
                if (state[yi][xi] == ban + 3) {
                    count++;
                    state[yi][xi] = MOVABLE;
                }
            } else {
                state[yi][xi] = MOVABLE;
                count++;
            }
            yi += yVec;
            xi += xVec;
        }
        return count;
    }

    public void changeAroundColor(Point p, int from, int to) {
        if (p.y > 0) {
            if (state[p.y - 1][p.x] == from) state[p.y - 1][p.x] = to;
            if (p.x > 0) {
                if (state[p.y - 1][p.x - 1] == from) state[p.y - 1][p.x - 1] = to;
            }
            if (p.x < 4) {
                if (state[p.y - 1][p.x + 1] == from) state[p.y - 1][p.x + 1] = to;
            }
        }
        if (p.y < 4) {
            if (state[p.y + 1][p.x] == from) state[p.y + 1][p.x] = to;
            if (p.x > 0) {
                if (state[p.y + 1][p.x - 1] == from) state[p.y + 1][p.x - 1] = to;
            }
            if (p.x < 4) {
                if (state[p.y + 1][p.x + 1] == from) state[p.y + 1][p.x + 1] = to;
            }
        }
        if (p.x > 0) {
            if (state[p.y][p.x - 1] == from) state[p.y][p.x - 1] = to;
        }
        if (p.x < 4) {
            if (state[p.y][p.x + 1] == from) state[p.y][p.x + 1] = to;
        }
    }

    public void displayState(){
        for(int y = 0; y < 5; y++) {
            for (int x = 0; x < 5; x++) {
                System.out.printf("%2d\t", state[y][x]);
                if (x < 4) System.out.print(",");
                else System.out.println("");
            }
        }
    }
}