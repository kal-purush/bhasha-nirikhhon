package hw.lesson8;

public class HashTableImpl<K, V> implements HashTable<K, V> {

    private final int maxSize;

    static class Node<K, V> implements Entry<K, V> {

        private final K key;
        private V value;

        public Node(K key, V value) {
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

        @Override
        public String toString() {
            return "Node{" +
                    "key=" + key +
                    ", value=" + value +
                    '}';
        }
    }


    private final Node<K, V>[] data;
    private int size;

    @SuppressWarnings("unchecked")
    public HashTableImpl(int maxSize) {
        this.maxSize = maxSize;
        this.data = new Node[this.maxSize * 2];
    }

    @Override
    public boolean put(K key, V value) {
        int index = hash(key);

        if (size == maxSize) {
            return false;
        }

        while(data[index] != null) {
            if (data[index].key.equals(key)) {
                data[index].setValue(value);
                return true;
            }

            index = index + getStep(key);
            index %= data.length;
        }

        data[index] = new Node<>(key, value);
        size++;
        return false;
    }

    protected int getStep(K key) {
        return 1;
    }

    private int hash(K key) {
        return key.hashCode() % data.length;
    }

    @Override
    public V get(K key) {
        int index = indexOf(key);
        return index == -1 ? null : data[index].getValue();
    }

    private int indexOf(K key) {
        int index = hash(key);
        while (data[index] != null) {
            Node<K, V> node = data[index];
            if (node.getKey().equals(key)) {
                return index;
            }
            index = index + getStep(key);
            index %= data.length;
        }

        return -1;
    }

    @Override
    public V remove(K key) {
        int index = indexOf(key);
        if (index == -1) {
            return null;
        }

        Node<K, V> node = data[index];
        data[index] = null;
        size--;
        return node.getValue();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public void display() {
        System.out.println("----------");
        for (int i = 0; i < data.length; i++) {
            System.out.printf("%d = [%s]%n", i, data[i]);
        }
        System.out.println("----------");
    }
}