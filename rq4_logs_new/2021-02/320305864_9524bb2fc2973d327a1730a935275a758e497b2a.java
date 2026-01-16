package daohuei.leetcodelizard;

/*
 * Question: Median of Two Sorted Arrays
 * Description: Given two sorted arrays nums1 and nums2 of size m and n respectively, return the median of the two sorted arrays.
 * The overall run time complexity should be O(log (m+n)).
 * 
 * Example 1:
 * Input: nums1 = [1,3], nums2 = [2]
 * Output: 2.00000
 * Explanation: merged array = [1,2,3] and median is 2.
 * 
 * Example 2:
 * Input: nums1 = [1,2], nums2 = [3,4]
 * Output: 2.50000
 * Explanation: merged array = [1,2,3,4] and median is (2 + 3) / 2 = 2.5.
 * 
 * Example 3:
 * Input: nums1 = [0,0], nums2 = [0,0]
 * Output: 0.00000
 * 
 * Example 4:
 * Input: nums1 = [], nums2 = [1]
 * Output: 1.00000
 * 
 * Example 5:
 * Input: nums1 = [2], nums2 = []
 * Output: 2.00000
 */
public class MedianTwoSortedArrays {
    /*
     * This function only serves for getting the median from an sorted array
     */
    public static double getMedian(int[] arr) {
        int n = arr.length;
        if (n % 2 == 0) {
            return (arr[n / 2 - 1] + arr[n / 2]) / 2.0;
        } else {
            return arr[n / 2];
        }
    }

    /*
     * Brutal Force Method
     * 
     * @Author: @daohuei
     * 
     * @Description: Merge two arrays directly then found the median number
     * 
     * @Time: O(m+n), run through two arrays separately.
     * 
     * @Space: O(m+n), we create another array with merged size: m+n
     */
    public static double findMedianSortedArraysBrutalForce(int[] nums1, int[] nums2) {
        int m = nums1.length;
        int n = nums2.length;
        if (n == 0) {
            return getMedian(nums1);
        }
        if (m == 0) {
            return getMedian(nums2);
        }

        int[] nums = new int[m + n];

        int count = 0;
        int i = 0, j = 0;
        while (count != (m + n)) {
            // if nums1 reach the end first
            if (i == m) {
                // push all the rest of the nums2 into result array
                while (j != n) {
                    nums[count++] = nums2[j++];
                }
                // then get rid of the while loop
                break;
            }
            // if nums2 reach the end of the while loop
            if (j == n) {
                while (i != m) {
                    nums[count++] = nums1[i++];
                }
                break;
            }

            // compare current value then only push the small one into result array
            if (nums1[i] < nums2[j]) {
                nums[count++] = nums1[i++];
            } else {
                nums[count++] = nums2[j++];
            }
        }

        return getMedian(nums);
    }

    /*
     * Binary Search Method
     * 
     * @Author: @daohuei
     * 
     * @Description: Use binary search for the shorter array and then compare the
     * maximum value on left and minimum value on right.
     * 
     * @Time: O(log(min(m+n))), since we only use binary search for the shorter
     * array, so the time complexity will be m or n(the minimal one)
     * 
     * @Space: O(1), since we are not using arrays to store result
     */
    public static double findMedianSortedArraysBinary(int[] nums1, int[] nums2) {
        int m = nums1.length;
        int n = nums2.length;
        if (n == 0) {
            return getMedian(nums1);
        }
        if (m == 0) {
            return getMedian(nums2);
        }
        if (m > n) {
            // Make sure the first array is the shorter one
            return findMedianSortedArraysBinary(nums2, nums1);
        }
        // Init the start curser with the first and the last point of the shorter array
        int start = 0;
        int end = m;

        // Loop until start and end met
        while (start <= end) {
            // The `nums1` index will be the center between `start` and `end`
            int i = (start + end) / 2;
            // The `nums2` index will be the complement of `i` from the total length
            // m+n+1(the amount of gaps)
            int j = (m + n + 1) / 2 - i;
            if (j > 0 && i < m && nums1[i] < nums2[j - 1]) {
                // If the left max of `nums2` is still larger than right min of `nums1`
                // we move start curser to the right position of the middle
                start = i + 1;

            } else if (i > 0 && j < n && nums2[j] < nums1[i - 1]) {
                // If the left max of `nums1` is still larger than right min of `nums2`
                // we move end curser to the left position of the middle
                end = i - 1;
            } else {
                // find the max value of left
                int maxLeft = 0;
                if (i == 0) {
                    // if i-1 out of index
                    maxLeft = nums2[j - 1];
                } else if (j == 0) {
                    // if j-1 out of index
                    maxLeft = nums1[i - 1];
                } else {
                    maxLeft = Math.max(nums1[i - 1], nums2[j - 1]);
                }
                if ((m + n) % 2 == 1) {
                    // If total length is odd
                    // The max value of left is the median
                    return maxLeft;
                }
                // find the min value of right
                int minRight = 0;
                if (i == m) {
                    // If i out of index
                    minRight = nums2[j];
                } else if (j == n) {
                    // If j out of index
                    minRight = nums1[i];
                } else {
                    minRight = Math.min(nums1[i], nums2[j]);
                }
                // If total length is even
                return (maxLeft + minRight) / 2.0;
            }
        }
        // If no answer found, we give the answer of 0.0
        return 0.0;
    }
}