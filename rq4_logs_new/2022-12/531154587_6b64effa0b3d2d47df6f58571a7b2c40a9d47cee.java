// Jump Game - https://leetcode.com/problems/jump-game/
class JumpGame {
    public boolean canJump(int[] nums) {
        //Grredy Approach
        int n=nums.length;
        //Set Last Element as True as this the expected result if from any point we reach here
        boolean[] ind = new boolean[n];
        ind[n-1] =true;
        //Traverse from last 
        for(int i=n-2;i>=0;i--){
            //If at any point index + value at index cross or eqaul to last true position of array then success  
            if(nums[i]+i>=n || ind[nums[i]+i]){
                ind[i] = true;
                n=i;
            } 
            //else false
            else ind[i] = false;
        }
        return ind[0];

    }
}