class Solution {
    public int[] decrypt(int[] code, int k) {
        int n = code.length;
        int[] result = new int[n];
        
        if (k == 0) {
            Arrays.fill(result, 0);
            return result;
        }

        int start = k > 0 ? 1 : n + k; // Start index for the sum window
        int end = k > 0 ? k : n - 1;  // End index for the sum window
        int sum = 0;

        // Calculate initial sum for the first element
        for (int i = start; i <= end; i++) {
            sum += code[i % n];
        }

        // Sliding window over the circular array
        for (int i = 0; i < n; i++) {
            result[i] = sum;
            sum -= code[start % n];
            start++;
            end++;
            sum += code[end % n];
        }

        return result;
    }
}