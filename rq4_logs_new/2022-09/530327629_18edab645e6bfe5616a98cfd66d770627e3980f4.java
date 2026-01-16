class Solution {
  //https://leetcode.com/problems/missing-number/
    public int missingNumber(int[] nums) {
        int i=0;
        while(i<nums.length){
            int correct=nums[i];
            if(correct==nums.length){
                i++;
                continue;
            }
            if(nums[i]!=nums[correct]){
                //swap
                swap(nums,i,correct);
            }else{
                i++;
            }
        }
        for(int j=0;j<nums.length; j++){
            if(nums[j]!= j){
                return j;
            }
        }
        return nums.length;
    }
   void  swap(int[] nums,int i,int correct){
         int temp=nums[correct];
       nums[correct]=nums[i];
       nums[i]=temp;
   }
}