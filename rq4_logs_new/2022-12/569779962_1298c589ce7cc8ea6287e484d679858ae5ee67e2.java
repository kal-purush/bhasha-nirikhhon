
// пусть дан linkedlist с несколькими элементами реализуйте метод который вернет перевернутый список
import java.util.*;

public class LinkedListTest2 {
    public static void main(String[] args)
    {
        LinkedList<Integer> linkedli = new LinkedList<Integer>();

        linkedli.add(new Integer(1));
        linkedli.add(new Integer(2));
        linkedli.add(new Integer(3));
        linkedli.add(new Integer(4));
        linkedli.add(new Integer(5));
        System.out.print("Elements before reversing: " + linkedli);

        linkedli = reverseLinkedList(linkedli);
        System.out.print(" Elements after reversing: " + linkedli);
    }

    public static LinkedList<Integer> reverseLinkedList(LinkedList<Integer> llist)
    {
        for (int i = 0; i < llist.size() / 2; i++) {
            Integer temp = llist.get(i);
            llist.set(i, llist.get(llist.size() - i - 1));
            llist.set(llist.size() - i - 1, temp);
        }

        return llist;
    }
}