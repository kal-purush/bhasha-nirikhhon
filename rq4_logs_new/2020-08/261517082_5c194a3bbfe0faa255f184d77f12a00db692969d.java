/*
        Return all non-negative integers of length N such that the absolute difference between every two consecutive digits is K.
        Note that every number in the answer must not have leading zeros except for the number 0 itself. 
        For example, 01 has one leading zero and is invalid, but 0 is valid.

        You may return the answer in any order.

        Example 1:
        Input: N = 3, K = 7
        Output: [181,292,707,818,929]
        Explanation: Note that 070 is not a valid number, because it has leading zeroes.

        Example 2:
        Input: N = 2, K = 1
        Output: [10,12,21,23,32,34,43,45,54,56,65,67,76,78,87,89,98]
        
        Note:
        1 <= N <= 9
        0 <= K <= 9
*/
class Solution {
        public int[] numsSameConsecDiff(int N, int K) {
                List<Integer> list = new LinkedList();
                if(N == 1){
                        return new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
                }
                
                for(int i = 1; i < 10; i++){
                        dfs(i, N-1, K,  list);
                }
                int[] ans = getArrayFromList(list);
                return ans;
        }
        
        private int[] getArrayFromList(List<Integer> list){
                int[] ans = new int[list.size()];
                for(int i = 0; i < list.size(); i++){
                        ans[i] = list.get(i);
                }
                return ans;
        }
        
        private void dfs(int val, int N, int K, List<Integer> list){
                if(N == 0){
                        list.add(val);
                        return;
                }
                int lastDigit = val % 10;
                
                if(lastDigit >= K && K != 0){
                        dfs(val*10 + (lastDigit - K), N - 1, K, list);
                }
                if(lastDigit+K < 10){
                        dfs(val*10 + (lastDigit+K), N - 1, K, list);    
                }
        }
}