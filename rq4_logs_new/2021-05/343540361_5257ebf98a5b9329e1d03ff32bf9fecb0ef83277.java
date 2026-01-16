package problem1048;

import java.util.ArrayList;
import java.util.List;

public final class LongestStringChain {

    public int longestStrChain(final String[] words) {
        if (words.length == 1)
            return 1;

        @SuppressWarnings("unchecked")
        final List<Integer>[] adjPos = new List[16];
        final int[] predCntLookup = new int[words.length];

        for (int i = 0; i < words.length; i++) {
            final int wPos = words[i].length() - 1;
            if (adjPos[wPos] == null)
                adjPos[wPos] = new ArrayList<>();
            adjPos[wPos].add(i);
        }

        int maxLen = 1;

        for (int i = 0; i < adjPos.length; i++) {
            final List<Integer> prev = i > 0 ? adjPos[i - 1] : null;
            final List<Integer> curr = adjPos[i];

            if (curr == null)
                continue;

            if (prev == null) {
                for (final int pos : curr)
                    predCntLookup[pos] = 1;
                continue;
            }

            for (int cPos : curr) {
                for (int pPos : prev) {
                    if (isPredecessor(words[pPos], words[cPos])) {
                        final int len = 1 + predCntLookup[pPos];
                        if (len > maxLen)
                            maxLen = len;
                        predCntLookup[cPos] = Math.max(predCntLookup[cPos], len);
                    } else {
                        predCntLookup[cPos] = Math.max(predCntLookup[cPos], 1);
                    }
                }
            }
        }

        return maxLen;
    }

    private boolean isPredecessor(final String shorter, final String longer) {
        boolean diffFound = false;
        for (int i = 0, j = 0; i < shorter.length(); i++, j++) {
            if (shorter.charAt(i) != longer.charAt(j)) {
                if (diffFound)
                    return false;
                diffFound = true;
                --i;
            }
        }
        return true;
    }

}