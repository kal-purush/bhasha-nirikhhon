// time - O(n)
// space -  O(1)


class Solution {
    public int minDominoRotations(int[] tops, int[] bottoms) {
        if(tops == null || tops.length == 0) return 0;
        System.out.println(tops[0]);
        int result = check(tops,bottoms,tops[0]);
        if(result != -1) return result;
        return check(tops,bottoms,bottoms[0]);
    }

    private int check(int[] tops, int[] bottoms, int target) {

        int aRots = 0;
        int bRots = 0;
        for(int i = 0; i < tops.length; i++) {
            if(tops[i] != target && bottoms[i] != target) return -1; // check if the element is not present at both locations return -1
            if(tops[i] != target) aRots++; // if the element is not present at top location increase a rotation by 1
            if(bottoms[i] != target) bRots++;  // if the element is not present at bottom location increase b rotation by 1

        }
        return Math.min(aRots, bRots); // find out min value between a rotation& b rotation
    }
}