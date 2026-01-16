/*
 * @lc app=leetcode id=1710 lang=java
 *
 * [1710] Maximum Units on a Truck
 */

// @lc code=start
class Solution {
    public int maximumUnits(int[][] boxTypes, int truckSize) {

        // Sort the boxTypes array based on the number of units in each box.
        sortBoxes(boxTypes);

        int maxUnits = 0;

        for(int i = 0; i < boxTypes.length; i++) {
            int boxes = boxTypes[i][0];
            int units = boxTypes[i][1];

            if(truckSize >= boxes) {
                maxUnits += boxes * units;
                truckSize -= boxes;
            } else {
                maxUnits += truckSize * units;
                break;
            }
        }

        return maxUnits;
        
       
        

        
    }

    static void sortBoxes(int[][] boxTypes){

        // sort the boxes according to the number of units in each box.

        Arrays.sort(boxTypes, new Comparator<int[]>(){
            public int compare(int[] a, int[] b){
                return b[1] - a[1];
            }
        });


    }
}
// @lc code=end
