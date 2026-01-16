public class SplitArray {
    public static void main(String[] args) {
        int[]arr={7,2,5,10,8};
     int ans= splitArray(arr,2);
    System.out.println(ans);
    }
    public static int splitArray(int[] nums, int m) {
int start=0;
int end=0;
for (int i = 0; i < nums.length; i++) {
    start=Math.max(start, nums[i]);
    end+=nums[i];

}

while(start<end){
    
int mid=start+(end-start)/2;
//try potential answer
int sum=0;
    int pieces=1;
for (int num : nums) {
    if(sum+num>mid){
        // make new subArray
        sum=num;
        pieces++;
    }else{
        sum+=num;
    }
   
}
if(pieces>m){
    // if pieces are more increase the mid value
    start=mid+1;
        }else{
    // pieces are smaller than m reduce the mid value
    end=mid;
        }
}
return end;// start=end
    }

  

}