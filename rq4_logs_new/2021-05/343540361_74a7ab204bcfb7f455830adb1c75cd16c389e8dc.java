package problem583;

public final class DeleteOperation {

    public int minDistance(final String word1, final String word2) {
        final int w1Len = word1.length();
        final int w2Len = word2.length();

        final char[] w1Ch = word1.toCharArray();
        final char[] w2Ch = word2.toCharArray();

        final int[][] lcsArr = new int[w1Len + 1][w2Len + 1];

        for (int i = 1; i < w1Len + 1; i++) {
            for (int j = 1; j < w2Len + 1; j++) {
                if (w1Ch[i - 1] == w2Ch[j - 1]) {
                    lcsArr[i][j] = lcsArr[i - 1][j - 1] + 1;
                } else {
                    lcsArr[i][j] = Math.max(lcsArr[i - 1][j], lcsArr[i][j - 1]);
                }
            }
        }

        return w1Len + w2Len - 2 * lcsArr[w1Len][w2Len];
    }

}