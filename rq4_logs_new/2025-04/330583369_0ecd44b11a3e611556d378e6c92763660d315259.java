package com.horadrim.staff.alg.tree;

/*
 * 字典树时间复杂度未O(m), m为字符串长度, 支持前缀查找但是内存消耗比较大
 */
public class Trie {
    public Trie() {
        root = new TrieNode();
    }

    public void insert(String word) {
        TrieNode current = root;
        for (char c : word.toCharArray()) {
            int index = (int) c;
            if (current.children[index] == null) {
                current.children[index] = new TrieNode();
            }
            current = current.children[index];
        }
        current.isEndOfWord = true;
    }

     // 检查一个单词是否存在于字典树中
     public boolean search(String word) {
        TrieNode current = root;
        for (char c : word.toCharArray()) {
            int index = (int) c;
            if (current.children[index] == null) {
                return false;
            }
            current = current.children[index];
        }
        return current.isEndOfWord;
    }

    // 检查一个字符串是否是字典树中某个字符串的前缀
    public boolean startsWith(String prefix) {
        TrieNode current = root;
        for (char c : prefix.toCharArray()) {
            int index = (int) c;
            if (current.children[index] == null) {
                return false;
            }
            current = current.children[index];
        }
        return true;
    }

    private TrieNode root;

    private class TrieNode {
        public TrieNode() {
            children = new TrieNode[256];
            isEndOfWord = false;
        }

        private TrieNode[] children;
        boolean isEndOfWord;
    }
}