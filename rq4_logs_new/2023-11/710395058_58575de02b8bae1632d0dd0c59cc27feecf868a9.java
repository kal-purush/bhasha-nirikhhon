package com.sunbeam;

public class DoubleCircularLinkedListMain {

	public static void main(String[] args) 
	{
		DoublyCircular dc=new DoublyCircular();
		
		dc.addFirst(1);
		dc.addFirst(2);
		dc.addLast(3);
        dc.addLast(4);
        dc.display();
        
        dc.addPosition(6, 2);
        dc.display();
        
        dc.deletePosition(2);
        dc.display();
	}

}
