package com.vmelnyk.geeks4geeks._20240130InsertAndSearchInTri;

import java.util.Arrays;

public class Solution {

  static final int ALPHABET_SIZE = 26;

  // trie node
  static class TrieNode {
    TrieNode[] children = new TrieNode[ALPHABET_SIZE];

    // isEndOfWord is true if the node represents
    // end of a word
    boolean isEndOfWord;

    TrieNode() {
      isEndOfWord = false;
      Arrays.fill(children, null);
    }
  }

  // Function to insert string into TRIE.
  static void insert(TrieNode root, String key) {
    // Your code here
    TrieNode node = root;
    int n = key.length(), i = 0;
    while (i < n) {
      final char c = key.charAt(i);
      final int idx = c - 'a';
      if (node.children[idx] == null) {
        node.children[idx] = new TrieNode();
      }
      node = node.children[idx];
      i++;

      if (i == n) {
        node.isEndOfWord = true;
      }
    }
  }

  // Function to use TRIE data structure and search the given string.
  static boolean search(TrieNode root, String key) {
    // Your code here
    TrieNode node = root;
    int n = key.length(), i = 0;
    while (i < n) {
      final char c = key.charAt(i);
      final int idx = c - 'a';

      if (node.children[idx] == null) {
        return false;
      }
      node = node.children[idx];
      i++;
    }

    return i == n && node.isEndOfWord;
  }
}