package Problems;

public class Problem1 {
    class Node{
        int data;
        Node next;
        public Node (int data){
            this.data = data;
            this.next = null;
        }
    }
    Node head = null;
    Node tail  = null;
    public void add(int data){
        Node newNode = new Node(data);
        if(head == null){
            head = newNode;
            tail = newNode;
        }
        else{
            newNode.next = head;
            head = newNode;
        }
    }

    public void count (){
        int count = 0;
        Node t = head;
        while(t != null){
            count++;
            t = t.next;
        }
        System.out.println("Number of Node in the LinkedList  :"+ count);
    }
    public void display(){
        Node temp = head;
        while(temp != null){
            System.out.print(temp.data+" ");
            temp = temp.next;
        }
    }



    public static void main(String[] args) {
        Problem1 p1 = new Problem1();
        p1.add(1);
        p1.add(2);
        p1.add(3);
        p1.display(); // 3 2 1
       p1.count(); //Number of Node in the LinkedList  :3

    }
}