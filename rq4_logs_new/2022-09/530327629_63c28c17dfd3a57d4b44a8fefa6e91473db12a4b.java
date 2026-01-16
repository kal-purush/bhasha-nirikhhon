class Solution {
    public int searchInsert(int[] nums, int target) {
       int ans= binarySearch(nums,target);
        return ans;
    }
    int binarySearch(int[] arr, int target){
        int start=0; int end=arr.length-1;
        while(start<=end){
            int mid=start+(end-start)/2;
            if(target==arr[mid]){
                return mid;
            }
            if(target>arr[mid]){
                start=mid+1;
            }
            if(target<arr[mid]){
                end=mid-1;
            }
        }
        return start;
    }
}