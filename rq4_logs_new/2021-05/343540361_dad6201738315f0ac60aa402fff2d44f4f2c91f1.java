package problem1423;

public final class MaximumCardsPoints {

    public int maxScore(final int[] cardPoints, final int k) {
        final int n = cardPoints.length;
        int curSum = 0, maxScore;
        for (int i = 0; i < k; i++)
            curSum += cardPoints[i];

        if (k == n)
            return curSum;

        maxScore = curSum;
        for (int i = k - 1, j = n - 1; i >= 0; i--, j--) {
            curSum += cardPoints[j] - cardPoints[i];
            if (curSum > maxScore)
                maxScore = curSum;
        }

        return maxScore;
    }

}