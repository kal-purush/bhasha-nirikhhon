package problem318;

public final class MaximumProductOfLengths {

    public int maxProduct(final String[] words) {
        final int[] wBin = new int[words.length];
        for (int i = 0; i < wBin.length; i++) {
            int bin = 0;
            for (var c : words[i].toCharArray())
                bin |= (1 << (c - 'a'));
            wBin[i] = bin;
        }

        int max = 0;
        for (int i = 0; i < words.length; i++) {
            for (int j = i + 1; j < words.length; j++) {
                if ((wBin[i] & wBin[j]) == 0) {
                    final int len = words[i].length() * words[j].length();
                    if (len > max)
                        max = len;
                }
            }
        }

        return max;
    }

}