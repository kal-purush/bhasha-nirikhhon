package dataStructures.pseudoQueue;

public class Driver {
    public static void main(String[] args) {
        PseudoQueue pseudoQueue = new PseudoQueue();   //  front{3} -> {7} -> {10}back
        pseudoQueue.enqueue(pseudoQueue , 3);    //
        pseudoQueue.enqueue(pseudoQueue , 7);
        pseudoQueue.enqueue(pseudoQueue , 10);


        System.out.println(pseudoQueue.dequeue(pseudoQueue)); //3
        System.out.println(pseudoQueue.dequeue(pseudoQueue)); //7
        System.out.println(pseudoQueue.dequeue(pseudoQueue)); //10
        // here we implement queue by using stack methods (push , pop)
    }
}