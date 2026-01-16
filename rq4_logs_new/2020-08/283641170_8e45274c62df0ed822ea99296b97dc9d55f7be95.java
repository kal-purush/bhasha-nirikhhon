//Intuition: I need a data structure that acts as key-value pair. Also I need a way to Order those pairs.
// I used double linked list for ordering purpose. each Node contains key and value pair

class LRUCache {
// private Node class which acts as a node containing key, value and pointers to next and prev nodes;
    private class Node {
        int key;
        int value;
        Node next;
        Node prev;
        Node(int key,int value){
            this.key = key;
            this.value = value;
        }
        Node() {}
    }
// this moveToHead function is used to move a node to the head postion indicate latest usage.
    void moveToHead(Node node) {
        removeNode(node);
        addToHead(node);
    }
// remove node used to remove a node from any given position. As a double linked list, I can remove it from any position.
    void removeNode(Node node) {
        Node one = node.prev;
        Node two = node.next;
        one.next = two;
        two.prev = one;
    }
//remove from tail.i.e, remove tail.prev; Just send that node to above function (removeNode())
    Node removeFromTail() {
        Node node = tail.prev;
        removeNode(node);
        return node;
    }
// add node to head position. i.e, right after head. indicate its as latest value.
    void addToHead(Node node) {
        Node ptr = head.next;
        head.next = node;
        node.prev = head;
        node.next = ptr;
        ptr.prev = node;
    }

    private int size;
    private Map<Integer,Node> cache = new HashMap<>();
    private Node head;
    private Node tail;

    public LRUCache(int capacity) {
        this.size = capacity;
        head = new Node();
        tail = new Node();
        head.next = tail;
        tail.prev = head;
    }


    public int get(int key) {
        if(cache.containsKey(key)){
            Node node = cache.get(key);
            moveToHead(node);
            return node.value;
        }
        else {
            return -1;
        }
    }

    public void put(int key, int value) {

        if(cache.containsKey(key)){
            Node node = cache.get(key);
            node.value = value;
            moveToHead(node);
        }
        else {
            if(cache.size() == size){
                Node node = removeFromTail();
                cache.remove(node.key);
            }
            Node node = new Node(key,value);
            cache.put(key,node);
            addToHead(node);
        }
    }
}

/**
 * Your LRUCache object will be instantiated and called as such:
 * LRUCache obj = new LRUCache(capacity);
 * int param_1 = obj.get(key);
 * obj.put(key,value);
 */

// time complexity: put and get O(1) operation.
