package hw.lesson8;

import java.util.LinkedList;

public class LinkedHashTableImpl<K, V> implements HashTable<K, V> {

    static class Position<K, V> implements Entry<K, V> {

        private final K key;

        private V value;

        public Position(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public void setValue(V value) {
            this.value = value;
        }
    }

    private final LinkedList<Position<K, V>>[] info;

    private int size;

    @SuppressWarnings("unchecked")
    public LinkedHashTableImpl(int maxSize) {
        this.info = new LinkedList[maxSize];
        for (int i = 0; i < info.length; i++) {
            info[i] = new LinkedList<>();
        }
    }

    private int hash(K key) {
        return key.hashCode() % info.length;
    }

    @Override
    public V get(K key) {
        int arrayIndex = hash(key);
        int listIndex = indexOf(key);
        return listIndex != -1 ? info[arrayIndex].get(listIndex).value : null;
    }

    private int indexOf(K key) {
        int index = hash(key);
        LinkedList<Position<K, V>> positions = info[index];
        for (int i = 0; i < positions.size(); i++) {
            Position<K, V> position = positions.get(i);
            if (position.key.equals(key)) {
                return i;
            }
        }

        return -1;
    }

    @Override
    public boolean put(K key, V value) {
        int index = hash(key);
        int listIndex = indexOf(key);
        if (listIndex != -1) {
            info[index].get(listIndex).value = value;
            return true;
        }
        info[index].add(new Position<>(key, value));
        size++;
        return true;
    }

    @Override
    public V remove(K key) {
        int arrayIndex = hash(key);
        int listIndex = indexOf(key);
        if (listIndex == -1) {
            return null;
        }
        Position<K, V> position = info[arrayIndex].remove(listIndex);
        size--;
        return position.value;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size != 0;
    }

    @Override
    public void display() {
        System.out.println("----------");
        for (int i = 0; i < info.length; i++) {
            System.out.printf("%d = [%s]%n", i, info[i]);
            System.out.println();
        }
        System.out.println("----------");
    }
}