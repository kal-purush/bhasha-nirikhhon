package com.cubeproblem.domain;

import java.util.InvalidPropertiesFormatException;

public class Cube{

    int[][][] cube;
    int N;

    public Cube(int N) throws InvalidPropertiesFormatException{
        if(1<= N && N<=100){
            this.cube = new int[N+1][N+1][N+1];
            this.N = N;
        } else {
            throw new InvalidPropertiesFormatException("Invalid value for cube construction");
        }
    }

    public String update(int x, int y, int z, int W){

        if(!(1<=x && x<=this.N && 1<=y && y<=this.N && 1<=z && z<=this.N)){
            return "Coordinates to update invalids";
        }

        if(!(Math.pow(-10,9)<= W && W<=Math.pow(10,9))){
            return "Invalid value";
        }

        this.cube[x][y][z] = W;
        return "";
    }

    public String query(int x1, int y1, int z1, int x2, int y2, int z2){

        if(!(1<=x1 && x1<= x2 && x2<=this.N && 1<=y1 && y1<=y2 && y2<=this.N && 1<=z1 && z1<=z2 && z2<=this.N)){
            return "Invalid Operation";
        }

        int suma = 0;

        for(int i= x1; i<=x2; i++){
            for(int j=y1; j<=y2; j++){
                for(int k=z1; k<=z2; k++){
                    suma+= this.cube[i][j][k];
                }
            }
        }
        return ""+suma;
    }
}