package BST;

/**
 * Created by alex on 24.10.15.
 */
public class BST <Key extends Comparable<Key>, Value> {

    private class Node {

        private Key key;
        private Value value;
        private Node left;
        private Node right;

        public Node(Key key, Value value) {
            this.key = key;
            this.value = value;
        }
    }

    public void put (Key key, Value val) {

    }

    public Value get(Key key) {

        return null;
    }

    public void delete(Key key) {

    }

    public Iterable<Key> iterator() {

        return null;
    }
 }