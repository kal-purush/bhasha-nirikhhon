class Solution {
    //binary Search solution
    public int findKthPositive(int[] arr, int k) {
        int start=0; int end=arr.length-1;
       
        
        while(start<=end){
            int mid=start+(end-start)/2;
            int missing=compute(arr[mid],mid+1);
            if(missing>=k){
                // move towards start
                end=mid-1;
            }else{
                //k is after this missing
                start=mid+1;
            }
        }
        if(end==-1){
            return k;
        }
        return arr[end]+k-compute(arr[end],end+1);
        
    }
    int compute(int actual,int expected){
        return actual-expected;
    }
}