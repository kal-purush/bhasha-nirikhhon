// Time Complexity : O(log (n-k))
// Space Complexity : O(1) 
// Did this code successfully run on Leetcode : Yes
// Any problem you faced while coding this : No


// Iterative: TC O(n), SC O(1)
// class Solution {
//     public List<Integer> findClosestElements(int[] arr, int k, int x) {
//         if(arr == null || arr.length ==0){
//             return new ArrayList<>();
//         }

//         int start = 0;
//         int end = arr.length - 1;
//         List<Integer> result = new ArrayList<>();
//         while(end - start + 1 > k){
//             int distS = x - arr[start];
//             int distE = arr[end] - x;

//             if(distS > distE){
//                 start++;
//             }
//             else{ // if distE is higher or euqal
//                 end--;
//             }
//         }
//         for(int i = start; i<=end; i++){
//             result.add(arr[i]);
//         }
//         return result;
//     }
// }

//BS: TC O(log(n-k)), SC O(1)
class Solution {
    public List<Integer> findClosestElements(int[] arr, int k, int x) {
        if(arr == null || arr.length ==0){
            return new ArrayList<>();
        }
        int n = arr.length;
        int low = 0;
        int high = n - k;
        List<Integer> result = new ArrayList<>();

        while(low < high){
            int mid = low + (high - low) / 2;
            int distS = x - arr[mid]; /// mid element is starting index
            int distE = arr[mid + k] - x;

            if(distS > distE){
                low = mid +1;
            }
            else{
                high = mid;
            }
        }
        for(int i = low; i < low + k; i++){
             result.add(arr[i]); 
        }
        return result;

    }
}