package be.upgrade.it.adventofcode2020.day3;

import be.upgrade.it.adventofcode2020.Utils;

public class TreeFinder {
    private final String[] map;
    final int[][] slopes = {
            {1,1},
            {3,1},
            {5,1},
            {7,1},
            {1,2},
    };

    public TreeFinder(String path) {
        this.map = Utils.resourceToStringList(path);
    }

    public int findTreesAtSlope(int right, int down){
        int x = 0;
        int y = 0;

        int trees = 0;

        while(y < map.length){
            String s = map[y];
            int i = x % s.length();

            if(s.charAt(i) == '#'){
                trees++;
            }

            x += right;
            y += down;
        }

        return trees;
    }

    public long checkAllSlopes(){
        long i = 1L;
        for (int[] slope : slopes) {
            int treesAtSlope = findTreesAtSlope(slope[0], slope[1]);
            i *= treesAtSlope;
        }
        return i;
    }
}