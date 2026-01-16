package LineEditor;

import java.awt.*;
import java.io.*;
import java.util.Scanner;

public class DLinkedList extends Component {
    private ElementDPtr tail;
    private String value;
    private ElementDPtr current;
    private ElementDPtr head;
    private int length = 0;

    public DLinkedList() {
        head = new ElementDPtr("", null, null);
        tail = new ElementDPtr("", head, null);
        head.setNext(tail);
        length = 0;
    }

    public DLinkedList(String v) {
        value = v;
        head = null;
        head.setNext(tail);
        length = 0;
    }

    public void help() {
        System.out.println("• • Insert before (b) String line\n" +
                "• • Insert after (a) String line\n" +
                "• • Delete (e) line\n" +
                "• • Replace (x) String line\n" +
                "• • Select (s) line\n" +
                "• • Find (f) String\n" +
                "• • Load (o) filename\n" +
                "• • Save (w) filename\n" +
                "• • Display (g)\n" +
                "• • Go top (i)\n" +
                "• • Go to end (t)\n" +
                "• • Go up (u)\n" +
                "• • Go down (d)\n" +
                "• • Find backward (v) String line\n" +
                "• • Find forward (n) String line\n" +
                "• • Delete lines (k) number line\n" +
                "• • Move line (y) lines line\n" +
                "• • Rename file (r) filename\n" +
                "• • Help (h)\n" +
                "• • Clear (c )\n" +
                "• • Quit (q)");
    }

    public void load() {
        Scanner scanner = new Scanner(System.in);
        String fileName = scanner.next();
        try {
            FileReader fileReader = new FileReader(fileName);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            Buffer textContent = new Buffer(this);
            while ((line = bufferedReader.readLine()) != null) {
                ElementDPtr e = new ElementDPtr(line);
                System.out.println(e.getValue());
            }
//          TODO: Append text to text area
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //    TODO: To write
    public void save() {

    }

    void insertBefore(int location, String value) {
//        Scanner scanner = new Scanner(System.in);
//        location = scanner.nextInt();
//        value = scanner.next();

        if (length == 0) {
            head = new ElementDPtr(value);
            tail = head;
        } else {
            ElementDPtr temp = head;
            for (int i = 1; i < location; i++) {
                temp = temp.getNext();
            }
            ElementDPtr e = new ElementDPtr(value);
            temp.getPrev().setNext(e);
            e.setPrev(temp.getPrev());
            e.setNext(temp);
            temp.setPrev(e);
        }
        length += 1;
        display();
    }

    void insertAfter(int location, String parameter) {
        if (length == 0) {
            head = new ElementDPtr(parameter);
            tail = head;
        } else {
            ElementDPtr temp = head;
            for (int i = 1; i < location; i++) {
                temp = temp.getNext();
            }
            ElementDPtr e = new ElementDPtr(parameter);
            e.setNext(temp.getNext());
            temp.getNext().setPrev(e);
            e.setPrev(temp);
            temp.setNext(e);
        }
        length += 1;
    }

    void delete(int location) {
        if (location == 0) {
            head = head.getNext();
            if (head == null)
                tail = null;
        }
        if (length == 0) {
            System.out.println("There's nothing to delete");
        } else {
            ElementDPtr temp = head;
            for (int i = 1; i < location; i++) {
                temp = temp.getNext();
            }
            temp.getPrev().setNext(temp.getNext());
            temp.getNext().setPrev(temp.getPrev());

        }
        length -= 1;

    }

    void replace(int location, String parameter) {
        if (location == 0) {
            insertBefore(0, parameter);
            delete(1);
        } else {
            ElementDPtr temp = head;
            for (int i = 1; i < location; i++) {
                temp = temp.getNext();
            }
            ElementDPtr e = new ElementDPtr(parameter);
            e.setNext(temp.getNext());
            e.setPrev(temp.getPrev());
            temp.getNext().setPrev(e);
            temp.getPrev().setNext(e);
            // delete();

        }
    }

//    TODO: To write
    void select(){}

//    TODO: To write
    void find(){}

    public void display() {
        if (head == null) {
            System.out.println("nothing");
        } else {
            ElementDPtr temp = head;
            while (temp != null)
                System.out.println(temp.getValue());
            temp = temp.getNext();
        }
    }

    //    Questionable method
    void goTop() {
        current = head;
    }

    //    Questionable method
    void goBottom() {
        current = tail;
    }

    void goUp() {
        current = current.getPrev();
    }

    void goDown() {
        current = current.getNext();
    }

    void findBackward(String pattern) {
        ElementDPtr temp = head;
        while (temp.getValue() != pattern && temp != current) {
            temp = temp.getNext();
            if (temp.getValue() == pattern) {
                current = temp;
            } else {
                System.out.println("STRING " + pattern + " NOT FOUND.");
            }
        }
    }

    void findForward(String pattern) {
        ElementDPtr temp = tail;
        while (temp.getValue() != pattern && temp != current) {
            temp = temp.getPrev();
            if (temp.getValue() == pattern) {
                current = temp;
            } else {
                System.out.println("STRING " + pattern + " NOT FOUND.");
            }
        }
    }

    void deleteLines(int start, int stop) {
        if ((start < 1) || (stop > length) || (start > length) || (stop < 1)) {
            System.out.println("INDICES OUT OF RANGE.");
        } else {
            for (int i = start; i <= stop; i++) {
                delete(start);
            }
        }
    }

    //    TODO: To write
    void quit() {
    }

//    TODO: Write
    void moveLine(){}

//    TODO: Write
    void renameFile(){}

//    TODO: Write
    void clear(){}
}