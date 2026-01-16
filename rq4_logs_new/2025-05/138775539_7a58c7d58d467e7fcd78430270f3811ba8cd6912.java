pakage my.com.app;

import java.util.*;

// Copy List with Random Pointer
//approach:
// 1. Create a mapping of original nodes to their copies using a HashMap.
// 2. Iterate through the original list and create a new node for each original node, storing the mapping in the HashMap.   
// 3. Iterate through the original list again to set the next and random pointers for the copied nodes using the HashMap.
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
        

        // create Node to Node mapping and keep it in hashmap
        // first iteration create a new LL 
        // second Iteration populate the randome index
        if ( head == null)
            return head;
        
        if( head.next == null){
            Node n = new Node(head.val);
            n.random = head.random == head ? n : null;
        }

        HashMap<Node, Node> hm = new HashMap();

        Node curr = head;
        Node copy_head = new Node(curr.val);
        hm.put(curr , copy_head);
        Node n = null;
        // check for random
        if ( curr.random != null && !hm.containsKey(curr.random)){
            n = new Node(curr.random.val);
            hm.put(curr.random , n);
        } else {
            if ( curr.random != null ){
                n = hm.get(curr.random);
            }
        }
        copy_head.random = n;
        
        Node prev = copy_head;
        while ( curr.next !=null ){
            
            if ( !hm.containsKey( curr.next )){ // create , not in HM
            
                Node temp = new Node(curr.next.val);
                prev.next = temp;
                hm.put(curr.next , temp);
                
            } else { 
                // Use from HM if not in HM
                prev.next = hm.get(curr.next);
            }
            
            prev.random = populateRandom(curr, hm);
            prev = prev.next;
            curr = curr.next;
        }

        prev.next = null;
        prev.random = populateRandom(curr, hm);

        return copy_head;
    }


    public Node populateRandom(Node curr, HashMap<Node, Node> hm ){
        Node n = null;
        // check for random
        if ( curr.random != null && !hm.containsKey(curr.random)){
            n = new Node(curr.random.val);
            hm.put(curr.random , n);
        } else {
            if ( curr.random != null ){
                n = hm.get(curr.random);
            }
        }
        return n;
    }


}