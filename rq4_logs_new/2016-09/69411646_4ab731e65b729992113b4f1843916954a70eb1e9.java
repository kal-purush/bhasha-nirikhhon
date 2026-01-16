package Moduler;

public class LinkedList {

    private Link first;
    private Link last;

    public LinkedList() {

        first = null;
        last = null;
    }

    public void insert(Link Item) {

        String data = Item.getString();
        Link previous = null;
        Link current = first;

        if (first == null) // if empty list,
        {
            first = Item; // first --> newLink
        } else {
            last.next = Item; // old last --> newLink
        }
        last = Item;
    }

    public boolean find(String key) {

        Link current = first;

        while (current != null) {

            if (current.getString().compareToIgnoreCase(key)==0) {
                return true;
                
            } else {
            
                current=current.next;
            }
        }
        return false;
    }

    public void displayList() {

        System.out.print("The list (Begining to end) ");
        Link current = first;

        while (current != null) {

            current.displayLink();
            current = current.next;
        }

        System.out.println(" ");
    }
}