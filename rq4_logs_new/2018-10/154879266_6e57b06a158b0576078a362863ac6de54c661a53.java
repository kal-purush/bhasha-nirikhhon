/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import com.*;
import ca.*;
/**
 *
 * @author lawal28a
 */

public class ListTest {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        ListElement le = new ListElement(); 
        linkedList temp = new linkedList();
        le.setData(0);
        temp.addElement(le);
        le.setData(1);
        temp.addElement(le);
        le.setData(2);
        temp.addElement(le);
        le.setData(3);
        temp.addElement(le);
        le.setData(4);
        temp.addElement(le);
        
        ListElement searchValue = new ListElement();
        searchValue = temp.getElement(3);
        searchValue.getData();
        
        System.out.print("Find data in the index 3: " + searchValue.getData() + "\n");
        
        System.out.print("Printing from tail to head: \n");
        temp.printLinkedListTail();
        
        System.out.print("Printing from head to tail: \n");
        temp.printLinkedListHead();
        
        temp.deleteElement(3);
        System.out.print("After deleting node: \n");
        temp.printLinkedListTail();
        
    }
    
}