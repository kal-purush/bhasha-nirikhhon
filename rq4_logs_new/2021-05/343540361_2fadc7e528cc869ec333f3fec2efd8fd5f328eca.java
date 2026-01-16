package problem745;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public final class WordFilter {

    private final Trie prefixTrie = new Trie();
    private final Trie suffixTrie = new Trie();

    public WordFilter(final String[] words) {
        for (int i = 0; i < words.length; i++) {
            final char[] wCh = words[i].toCharArray();
            prefixTrie.addWord(wCh, i);
            suffixTrie.addWordInReverse(wCh, i);
        }
    }

    public int f(final String prefix, final String suffix) {
        final List<Integer> pIdx = prefixTrie.getIdxList(prefix.toCharArray());
        if (pIdx == null || pIdx.isEmpty())
            return -1;

        final List<Integer> sIdx = suffixTrie.getIdxListInReverse(suffix.toCharArray());
        if (sIdx == null || sIdx.isEmpty())
            return -1;

        final Iterator<Integer> pIter = pIdx.iterator();
        final Iterator<Integer> sIter = sIdx.iterator();

        int p = pIter.next();
        int s = sIter.next();
        for (;;) {
            if (p == s)
                return p;
            else if (p > s)
                if (pIter.hasNext())
                    p = pIter.next();
                else
                    return -1;
            else
                if (sIter.hasNext())
                    s = sIter.next();
                else
                    return -1;
        }
    }


    private static final class Trie {
        private final Trie[] tries = new Trie['z' - 'a' + 1];
        private LinkedList<Integer> idxList = new LinkedList<>();

        void addWord(final char[] word, final int idx) {
            Trie node = this;
            for (final char c : word) {
                final int chIdx = c - 'a';
                if (node.tries[chIdx] == null)
                    node.tries[chIdx] = new Trie();
                node = node.tries[chIdx];
                node.idxList.addFirst(idx);
            }
        }

        void addWordInReverse(final char[] word, final int idx) {
            Trie node = this;
            for (int i = word.length - 1; i >= 0; i--) {
                final int chIdx = word[i] - 'a';
                if (node.tries[chIdx] == null)
                    node.tries[chIdx] = new Trie();
                node = node.tries[chIdx];
                node.idxList.addFirst(idx);
            }
        }

        List<Integer> getIdxList(final char[] word) {
            Trie node = this;
            for (final char c : word) {
                final int chIdx = c - 'a';
                if (node.tries[chIdx] == null)
                    return null;
                node = node.tries[chIdx];
            }
            return node.idxList;
        }

        List<Integer> getIdxListInReverse(final char[] word) {
            Trie node = this;
            for (int i = word.length - 1; i >= 0; i--) {
                final int chIdx = word[i] - 'a';
                if (node.tries[chIdx] == null)
                    return null;
                node = node.tries[chIdx];
            }
            return node.idxList;
        }
    }

}