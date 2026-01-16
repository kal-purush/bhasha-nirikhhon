public class Solution {
    public int maxEqualRowsAfterFlips(int[][] matrix) {
        Map<String, Integer> patternCount = new HashMap<>();
        int maxCount = 0;

        for (int[] row : matrix) {
            StringBuilder pattern = new StringBuilder();
            StringBuilder flippedPattern = new StringBuilder();

            for (int val : row) {
                pattern.append(val);
                flippedPattern.append(1 - val); // Flip the value
            }

            // Use the canonical representation
            String key = pattern.toString();
            String flippedKey = flippedPattern.toString();

            patternCount.put(key, patternCount.getOrDefault(key, 0) + 1);
            maxCount = Math.max(maxCount, patternCount.get(key));

            patternCount.put(flippedKey, patternCount.getOrDefault(flippedKey, 0) + 1);
            maxCount = Math.max(maxCount, patternCount.get(flippedKey));
        }

        return maxCount;
    }
}