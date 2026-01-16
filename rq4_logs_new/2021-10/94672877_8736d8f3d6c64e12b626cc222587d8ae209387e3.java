package com.kuldeep.data.structure.tree;

import java.util.Objects;

/**
 * @author <a href="kuldeepyadav7291@gmail.com">Kuldeep</a>
 */
public class TrieNode {
    private final TrieNode[] nodes;
    private boolean endOfWord;
    private String word;

    private TrieNode() {
        this.nodes = new TrieNode[26];
    }

    public static TrieNode create() {
        return new TrieNode();
    }


    public void insert(String word) {
        TrieNode current = this;
        for (int i = 0; i < word.length(); i++) {
            int idx = getIdx(word.charAt(i));

            if (Objects.isNull(current.nodes[idx])) {
                current.nodes[idx] = TrieNode.create();
            }

            current = current.nodes[idx];
        }

        current.endOfWord = true;
        current.word = word;
    }

    public boolean contains(String word) {
        TrieNode current = this;

        for (int i = 0; i < word.length(); i++) {
            int idx = getIdx(word.charAt(i));

            if (Objects.isNull(current.nodes[idx])) return false;

            current = current.nodes[idx];
        }

        return current.endOfWord && current.word.equals(word);
    }

    private int getIdx(char ch) {
        return Character.toLowerCase(ch) - 'a';
    }

    public boolean containsPrefix(String prefix) {
        TrieNode current = this;

        for (int i = 0; i < prefix.length(); i++) {
           int idx = getIdx(prefix.charAt(i));

           if (Objects.isNull(current.nodes[idx])) return false;
           current = current.nodes[idx];
        }

        return true;
    }
}