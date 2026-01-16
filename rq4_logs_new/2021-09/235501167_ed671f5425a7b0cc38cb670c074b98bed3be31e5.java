package hard;

import java.util.Stack;

/**
 * https://leetcode.com/problems/maximal-rectangle/
 *
 * Given a rows x cols binary matrix filled with 0's and 1's, find the largest rectangle containing only 1's and return its area.
 */
public class Maximal_Rectangle {
    class Solution {
        int maxArea = 0;
        public int maximalRectangle(char[][] mat) {
            int m = mat.length;
            if(m == 0) return 0;

            int n = mat[0].length;

            if(n==0) return 0;

            int[] prev = new int[n];
            for(int i = 0; i < n; i++) {
                prev[i] = mat[0][i] == '0' ? 0 : 1;
            }

            maxArea = getMaxArea(prev);

            for(int i = 1; i < m; i++) {
                for(int j = 0; j < n; j++) {
                    if(mat[i][j] == '0') {
                        prev[j] = 0;
                    } else{
                        prev[j] = prev[j] + 1;
                    }
                }
                maxArea = Math.max(maxArea, getMaxArea(prev));
            }

            return maxArea;
        }

        int getMaxArea(int[] nums) {
            int i = 0;
            int area = 0;
            Stack<Integer> st = new Stack<>();

            while(i < nums.length) {
                if(st.isEmpty() || nums[i] >= nums[st.peek()]) {
                    st.push(i++);
                } else {
                    int top = st.pop();
                    if(st.isEmpty()) {
                        area = Math.max(area, nums[top] * i);
                    } else {
                        area = Math.max(area, nums[top] * (i - 1 - st.peek()));
                    }
                }
            }

            while(!st.isEmpty()) {
                int top = st.pop();
                if(st.isEmpty()) {
                    area = Math.max(area, nums[top] * i);
                } else {
                    area = Math.max(area, nums[top] * (i - 1 - st.peek()));
                }
            }

            return area;
        }
    }
}