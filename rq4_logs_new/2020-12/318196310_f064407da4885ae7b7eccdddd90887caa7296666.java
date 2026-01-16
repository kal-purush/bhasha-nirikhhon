package be.upgrade.it.adventofcode2020.day11;

import java.util.Arrays;

public class SeatingSystem {
    private char[][] seatingChart;

    public SeatingSystem(String[] input) {
        seatingChart = new char[input.length][];
        for (int i = 0; i < input.length; i++) {
            seatingChart[i]=input[i].toCharArray();
        }
    }

    public int getOccupiedSeats(){
        int count = 0;
        for (char[] chars : seatingChart) {
            for (char c : chars) {
                if(c == '#'){
                    count++;
                }
            }
        }
        return count;
    }

    public boolean doRound(boolean countFloorAsAdjacent){
        int before = getOccupiedSeats();

        char[][] clone = new char[seatingChart.length][seatingChart[0].length];

        for (int i = 0; i < seatingChart.length; i++) {
            for (int j = 0; j < seatingChart[i].length; j++) {

                switch (seatingChart[i][j]){
                    case '.':
                        clone[i][j] = '.';
                        break;
                    case 'L':
                        if(countAdjacentOccupied(i, j, countFloorAsAdjacent) == 0){
                            clone[i][j] = '#';
                        } else {
                            clone[i][j] = 'L';
                        }
                        break;
                    case '#':
                        if(countAdjacentOccupied(i,j, countFloorAsAdjacent) >= 4 && countFloorAsAdjacent){
                            clone[i][j] = 'L';
                        } else if(countAdjacentOccupied(i,j, countFloorAsAdjacent) >= 5 && !countFloorAsAdjacent){
                            clone[i][j] = 'L';
                        } else{
                            clone[i][j] = '#';
                        }
                        break;
                }
            }
        }

        seatingChart = clone;

        int current = getOccupiedSeats();

        return current == before;
    }

    private int countAdjacentOccupied(int i, int j, boolean countFloorAsAdjacent) {
        int[][] adjacentCoordinates = {
                {-1, -1},
                {-1, 0},
                {-1, +1},
                {0, -1},
                {0, +1},
                {+1, -1},
                {+1, 0},
                {+1, +1},
        } ;
        int count = 0;
        for (int[] coordinate : adjacentCoordinates) {
            try {
                int x = coordinate[0];
                int y = coordinate[1];

                if(!countFloorAsAdjacent){
                    while (seatingChart[i+x][j+y] == '.'){
                        x += coordinate[0];
                        y += coordinate[1];
                    }
                }

                char c = seatingChart[i+x][j+y];
                if(c == '#'){
                    count++;
                }
            } catch (Throwable e) {
                //e.printStackTrace();
            }
        }
        return count;
    }


}