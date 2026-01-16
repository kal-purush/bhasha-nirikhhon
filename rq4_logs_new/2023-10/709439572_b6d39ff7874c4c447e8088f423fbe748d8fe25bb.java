// This is the solution of the Trapping Rain Water problem.
public class prajwalgandhi {
    //TIME COMPLEXITY - O(n )
    static void trapping_water(int[] height){
        // size of the array
        int n = height.length;

        int[] leftMax = new int[height.length];
        int[] rightmax = new int[height.length];

        leftMax[0] = height[0];
        // left Max
        for(int i=1;i<height.length;i++){
            leftMax[i] = Math.max(height[i], leftMax[i-1]);
        }

        rightmax[height.length-1] = height[height.length-1];
        // right Max
        for(int i=height.length-2;i>=0;i--){
            rightmax[i] = Math.max(height[i],rightmax[i+1]);
        }

        int trappedWater = 0;
        //loop
        for(int i=0;i<n;i++){
            int waterLevel = Math.min(leftMax[i], rightmax[i]);
            
            //trapped water = waterLevel - height[i]
            trappedWater +=  waterLevel - height[i];
        }
        System.out.println("Total Trapped Water: "+trappedWater);
    }
    public static void main(String[] args) {
        int[] arr = {4,2,0,6,3,2,5};
        trapping_water(arr);
    }
}