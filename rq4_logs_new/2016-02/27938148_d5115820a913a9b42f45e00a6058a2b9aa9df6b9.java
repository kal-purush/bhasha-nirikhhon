package shildt.book.exampleCollections;

import java.util.Enumeration;
import java.util.Hashtable;

/**
 * Created by luchkovsky on 16.02.2016.
 */
public class HashTableDemo {
    public static void main(String[] args) {
        Hashtable<String, Double> balance = new Hashtable<String, Double>();

        Enumeration<String> names;

        String str;
        double bal;

        balance.put("test1", 123.43);
        balance.put("test2", 3.6);
        balance.put("test3", 2000.002);

        //show all bills in hash table
        names = balance.keys();
        while (names.hasMoreElements()){
            str = names.nextElement();
            System.out.println(str + ": " + balance.get(str));
        }
        System.out.println();

        //add 5555 in bill test1
        bal = balance.get("test1");
        balance.put("test1", bal + 5555);
        System.out.println("new balance test1: " + balance.get("test1"));
    }
}