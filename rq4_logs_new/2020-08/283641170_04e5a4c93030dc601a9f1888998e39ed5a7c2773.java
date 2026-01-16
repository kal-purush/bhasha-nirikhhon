/*
// Definition for a Node.
class Node {
    int val;
    Node next;
    Node random;

    public Node(int val) {
        this.val = val;
        this.next = null;
        this.random = null;
    }
}
*/

class Solution {
    public Node copyRandomList(Node head) {
        Map<Node,Node> map = new HashMap<>();
        Node dummy = head;
        while(dummy != null){
            map.put(dummy, new Node(dummy.val));
            dummy = dummy.next;
        }

        Node newHead = map.get(head);
        Node ptr = newHead;
        while(head != null){
            newHead.next = map.get(head.next);
            newHead.random = (head.random == null)? null:map.get(head.random);
            newHead = newHead.next;
            head = head.next;
        }
        return ptr;
    }
}
// time complexity : O(n)
// space complexity: O(n)

// intuition: create a mapping of Node, new Node. iterate through the first list and assign the new new Nodes from the map.