class Solution {
    public int firstMissingPositive(int[] nums) {
        int i=0;
        while(i<nums.length){
            int correct=nums[i]-1;
            if(correct>=0 && correct<nums.length){
                      if( nums[i]!= nums[correct]){
                //swap
                swap(nums, i,correct);
            }
                else{
                i++;
            }
            }else{
                i++;
            }
            
      
        }
        for(int index=0; index<nums.length;index++){
            if(nums[index]!= index+1){
                return index+1;
            }
        }
        return nums.length+1;
    }
    void swap(int[]nums, int i, int correct){
        int temp=nums[correct];
        nums[correct]=nums[i];
        nums[i]=temp;
    }
}