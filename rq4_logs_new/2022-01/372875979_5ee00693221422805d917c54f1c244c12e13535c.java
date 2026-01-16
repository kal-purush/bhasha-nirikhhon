import java.util.HashMap;

/**
 * @author : dheerajkumar02
 * @date : 26-12-2021
 * @project : data-structure
 */
public class HashMapCustom<K, V> {

    Node<K, V>[] obj;
    int currSize;
    int size;


    static class Node<K, V> {
        final V val;
        final K key;
        Node next;

        Node(K key, V val) {
            this.val = val;
            this.key = key;
            next = null;
        }

        @Override
        public String toString() {
            return "Node{" +
                    "val=" + val +
                    ", key=" + key +
                    ", next=" + next +
                    '}';
        }
    }

    HashMapCustom(int size) {
        obj = new Node[size];
        currSize = 0;
        this.size = size;
    }

    Node<K,V> newNode(K key, V value) {
        return new Node<>(key, value);
    }

    public void put(K key, V value) {
        int index = hash(key);
        Node<K, V> node = new Node<>(key, value);
        if (obj[index] == null) {
            obj[index] = node;
        } else {
            if (obj[index] instanceof Node) {
                Node<K, V> temp = obj[index];
                Node<K, V> curr = temp;
                while (curr.next != null) {
                    curr = curr.next;
                }
                curr.next = node;
            }
       }
        currSize++;
    }

    private int hash(Object key) {
        return (key.hashCode() % size);
    }

       /* private int index(int hash){

        return i;
        }*/

    public Object get(Object key) {
        currSize--;
        return obj[hash(key)];
    }

    public static void main(String[] args) {
        HashMapCustom hmc = new HashMapCustom(10);
        Object key = new Integer(1);
        Object value = new String("One");
        hmc.put(key, value);
        System.out.println(hmc.get(key).toString());
        Object key1 = new Integer(1);
        Object value1 = new String("Two");
        hmc.put(key1, value1);
        System.out.println(hmc.get(key1).toString());
    }
}