class Solution {
    public int findLengthOfShortestSubarray(int[] arr) {
        int length = arr.length;
        int left = 0;

        // Find the longest non-decreasing prefix
        while (left < length - 1 && arr[left] <= arr[left + 1]) {
            left++;
        }
        
        // If the whole array is already sorted
        if (left == length - 1) {
            return 0;
        }
        
        // Find the longest non-decreasing suffix
        int right = length - 1;
        while (right > 0 && arr[right - 1] <= arr[right]) {
            right--;
        }

        int result = Math.min(length - left - 1, right);
        for (int i = 0; i <= left; i++) {
            while (right < length && arr[i] > arr[right]) {
                right++;
            }
            result = Math.min(result, right - i - 1);
        }
        
        return result;
    }
}