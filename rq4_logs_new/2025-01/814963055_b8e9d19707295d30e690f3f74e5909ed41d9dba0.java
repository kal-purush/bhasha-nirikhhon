package com.example.multithreadingProject.LRUCache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LRUCache<K,V> {
    private final Map<K, Node<K,V>> cache;
    private final int capacity;
    private final Node<K,V> head, tail;

    public LRUCache(int capacity) {
        this.capacity = capacity;
        cache = new ConcurrentHashMap<>();
        head = new Node<>(null, null);
        tail = new Node<>(null, null);
        head.next = tail;
        tail.prev = head;

    }

    public synchronized void put(K key, V value) {
        if (cache.containsKey(key)) {
            Node<K,V> node = cache.get(key);
            node.value = value;
            moveToHead(node);
        } else {
            Node<K,V> node = new Node<>(key, value);
            cache.put(key, node);
            addToHead(node);
            if (cache.size() > capacity) {
                Node<K,V> removedNode = removeTail();
                cache.remove(removedNode.key);
            }
        }
    }

    public synchronized V get(K key) {
        if (cache.containsKey(key)) {
            Node<K,V> node = cache.get(key);
            moveToHead(node);
            return node.value;
        } else {
            return null;
        }
    }

    void addToHead(Node<K,V> node) {
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
        node.prev = head;
    }

    void moveToHead(Node<K,V> node) {
        removeNode(node);
        addToHead(node);
    }

    void removeNode(Node<K,V> node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    public Node<K,V> removeTail() {
        Node<K,V> node = tail.prev;
        removeNode(node);
        return node;
    }
}