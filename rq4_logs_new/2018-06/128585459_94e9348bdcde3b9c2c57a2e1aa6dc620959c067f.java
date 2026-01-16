public class No80 {

    private int removeDuplicates(int[] nums) {
        int n = 1;
        for (int i = 1, count = 1; i < nums.length; i++){
            if (nums[i] != nums[i-1]) {
                nums[n++] = nums[i];
                count = 1;
            }
            else if (nums[i] == nums[i-1] && count < 2){
                nums[n++] = nums[i];
                count ++;
            }
            else
                count ++;
        }

        return n;
    }

    public static void main(String[] args){
        int[] nums = {1,1,1};
        System.out.println(new No80().removeDuplicates(nums));
        for (int i : nums)
            System.out.print(i + " ");
    }
}