//members: Mark Gabriel Chiong, Vanessa Amandoron, & Joshua Tabequero
import java.util.*;
import java.util.Random;
import javax.swing.*;
import java.text.*;
import java.lang.Math;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.io.File;
public class rssm
{	
	
 Node waiter1;  // head of list
 static class Node {
		String data;
        Node next;
		Node(String d)  { 
		data = d;  next=null; 
		}
     }
     /* This function prints contents of the linked list starting from head */
 public void display()
     {
         Node n = waiter1;
System.out.println("Waiter Income:\n");
 while (n != null)
         {
 System.out.print(n.data+" \n");
             n = n.next;
         }
     }
	 
 public static void main(String[] args)
     {
		
		 Scanner scan = new Scanner(System.in);
		 System.out.println("Welcome to Restaurant Staff Status Management System");
		 System.out.println("Enter login details to verify user: ");
		 
		 
		 String un = JOptionPane.showInputDialog(null,"\nUsername: ","Login",JOptionPane.INFORMATION_MESSAGE);
		 		
		 String pw = JOptionPane.showInputDialog(null,"Password: ","Login",JOptionPane.INFORMATION_MESSAGE);
		 
		if (un.equals("maam amor") || pw.equals("ccd2020")){
			System.out.println("_____________________________________________");
			System.out.println("_____________________________________________");
			System.out.println();
			System.out.println();
			System.out.println("\nACCESS GRANTED!!\n");
			 System.out.println("Restaurant Management System \n ");
			 greetings();
			 System.out.println();
			 System.out.println();
			 System.out.println();
			 System.out.println();
			 System.out.println("_____________________________________________");
			 System.out.println("_____________________________________________");
			 	int r=1;
				Object[] options = { "20", "30","40"};
				int choice = JOptionPane.showOptionDialog(null,"Please select an option","Restaurant Tables?",JOptionPane.YES_NO_CANCEL_OPTION,
				JOptionPane.QUESTION_MESSAGE,null,options,options[0]);

				if (choice == JOptionPane.YES_OPTION){
				twenty();
				}
				else if(choice == JOptionPane.NO_OPTION){
				thirty();
				}
				else if(choice == JOptionPane.CANCEL_OPTION){
				forty();
				}
				else {
				System.out.println("Please Select an Option");
				}
		 }
		else{
				//String gab = JOptionPane.showMessageDialog("Invalid Input!","LOGIN FAILED",JOptionPane.INFORMATION_MESSAGE);
			System.out.println("Failed");
		} 
		
	 }
	 //DATE AND TIME APPLICATION
	 static void greetings(){
		 Date date = new Date();
		 
		 SimpleDateFormat Hours = new SimpleDateFormat("HH:mm:ss");
		 SimpleDateFormat Months =  new SimpleDateFormat("dd/MM/yyyy");
		 
		 System.out.println("Greetings! Today is " +Months.format(date)+"\nTIME CHECK: "+Hours.format(date));
	 }
			 
	static void twenty(){		//Method for 20 Tables	 
	     /* Start with the empty list. */
	int n;
	Scanner scan = new Scanner(System.in);
	 String[] wname = new String[5];
	 System.out.println("_____________________________________________");
	 for (int i=0; i<5; i++){
				 System.out.println("\nEnter Waiter"+(i+1)+" name: ");
				 wname[i] = scan.next();
			 }
			 System.out.println("_____________________________________________");
	Random rand = new Random();
	 
	 int inc1 = rand.nextInt((5000)+1000);
	 int inc2 = rand.nextInt((5000)+1000);
	 int inc3 = rand.nextInt((5000)+1000);
	 int inc4 = rand.nextInt((5000)+1000);
	 int inc5 = rand.nextInt((5000)+1000);

 rssm list = new rssm();  // LINKED LIST APPLICATION 

		 list.waiter1 = new Node("Waiter 1: ("+wname[0]+")	 Income: "+ inc1); 
         Node waiter2 = new Node("Waiter 2: ("+wname[1]+")	 Income: "+ inc2);
         Node waiter3 = new Node("Waiter 3: ("+wname[2]+")	 Income: "+ inc3);
		 Node waiter4 = new Node("Waiter 4: ("+wname[3]+")	 Income: "+ inc4);
		 Node waiter5 = new Node("Waiter 5: ("+wname[4]+")	 Income: "+ inc5);
		 
	 list.waiter1.next = waiter2; 
	 waiter2.next = waiter3; 
	 waiter3.next = waiter4;
	 waiter4.next = waiter5;
	 list.display();
	 int[] arr = {inc1, inc2, inc3, inc4, inc5};
	 selection(arr,wname);

	 
	 
	

	}	
	public static void thirty(){	 // Method for 30 Tables		 
	      /* Start with the empty list. */
	int n;
	Scanner scan = new Scanner(System.in);
	 String[] wname = new String[5];
	 System.out.println("_____________________________________________");
	 for (int i=0; i<5; i++){
				 System.out.println("\nEnter Waiter"+(i+1)+" name: ");
				 wname[i] = scan.next();
			 }
			 System.out.println("_____________________________________________");
	Random rand = new Random();

	 int inc1 = rand.nextInt(15000)+10000;
	 int inc2 = rand.nextInt(15000)+10000;
	 int inc3 = rand.nextInt(15000)+10000;
	 int inc4 = rand.nextInt(15000)+10000;
	 int inc5 = rand.nextInt(15000)+10000;

 rssm list = new rssm();  

		 list.waiter1 = new Node("Waiter 1: ("+wname[0]+")	 Income: "+ inc1); 
         Node waiter2 = new Node("Waiter 2: ("+wname[1]+")	 Income: "+ inc2);
         Node waiter3 = new Node("Waiter 3: ("+wname[2]+")	 Income: "+ inc3);
		 Node waiter4 = new Node("Waiter 4: ("+wname[3]+")	 Income: "+ inc4);
		 Node waiter5 = new Node("Waiter 5: ("+wname[4]+")	 Income: "+ inc5);
			 
	 list.waiter1.next = waiter2; 
	 waiter2.next = waiter3; 
	 waiter3.next = waiter4;
	 waiter4.next = waiter5;
	System.out.println("_____________________________________________");
	 list.display();
	 int[] arr = {inc1, inc2, inc3, inc4, inc5};
	 selection(arr,wname);
	 
	
		 
	}
	
	public static void forty(){	//Method for 40 Tables
					
         /* Start with the empty list. */
	int n;
	Scanner scan = new Scanner(System.in);
	 String[] wname = new String[5];
	 System.out.println("_____________________________________________");
	 for (int i=0; i<5; i++){
				 System.out.println("\nEnter Waiter"+(i+1)+" name: ");
				 wname[i] = scan.next();
			 }
	Random rand = new Random();

	 int inc1 = rand.nextInt(20000)+15000;
	 int inc2 = rand.nextInt(20000)+15000;
	 int inc3 = rand.nextInt(20000)+15000;
	 int inc4 = rand.nextInt(20000)+15000;
	 int inc5 = rand.nextInt(20000)+15000;

	rssm list = new rssm();  

		  list.waiter1 = new Node("Waiter 1: ("+wname[0]+")	 Income: "+ inc1); 
         Node waiter2 = new Node("Waiter 2: ("+wname[1]+")	 Income: "+ inc2);
         Node waiter3 = new Node("Waiter 3: ("+wname[2]+")	 Income: "+ inc3);
		 Node waiter4 = new Node("Waiter 4: ("+wname[3]+")	 Income: "+ inc4);
		 Node waiter5 = new Node("Waiter 5: ("+wname[4]+")	 Income: "+ inc5);
		
	list.waiter1.next = waiter2; 
	waiter2.next = waiter3; 
	waiter3.next = waiter4;
	waiter4.next = waiter5;
	list.display();
	int[] arr = {inc1, inc2, inc3, inc4, inc5};
	 selection(arr,wname);
	System.out.println("_____________________________________________");

	}
	
	public static int[] selection(int[] a, String[] n){ //selection sorting algorithm method
		 Scanner scan = new Scanner(System.in);
		 Random rand = new Random();
		 Date date = new Date();
		 
		 SimpleDateFormat Hours = new SimpleDateFormat("HH:mm:ss");
		 SimpleDateFormat Months =  new SimpleDateFormat("dd/MM/yyyy");
		 
		int[] waiter = {1,2,3,4,5};
		
		
		for (int i=0; i<=5-1; i++){
			int minIndex = i;
			for (int j=i+1; j<5; j++){
			if(a[j] < a[minIndex] ){
				minIndex = j;
				int temp1 = waiter[j];
				waiter[j]=waiter[i]; 
				waiter[i] = temp1;
				
				String temp2 = n[j];
				n[j] = n[i];
				n[i] = temp2;
				
				}
			}
		int temp = a[minIndex];
		a[minIndex] = a[i];
		a[i] = temp;
		}
		//SORTED WAITERS IN RANKING
		System.out.println("_____________________________________________");
		System.out.println("\nRanking of Waiters According to Income:\n");
		for(int i=0 ; i<5; i++){
			 System.out.println("Waiter "+waiter[i]+ ": (" +n[i]+")	  Income: $"+ a[i]);
		}
		//SORTED WAITER WITH TIPS
		System.out.println("_____________________________________________");
		System.out.println("\nRanking of Waiters According to Income with 10% tip of their income:\n");
			double tip = 0.10;
		for(int i=0 ; i<5; i++){
			 System.out.println("Waiter "+waiter[i]+ ": (" +n[i]+")	  Income: $"+ (a[i]+(a[i]*tip)));
		}
		
		//DEQUEUEING THE WAITER WITH HIGHEST INCOME 
		System.out.println("_____________________________________________");
		System.out.println("\nWaiter for Promotion: \n\nWaiter "+waiter[4]+ ": (" +n[4]+")	  Income: $"+(a[4]+(a[4]*tip)));
		System.out.println("\nDate of promotion: "+Months.format(date));
		System.out.println("_____________________________________________");
		
		//REMAINING WAITERS ON THE LIST
		System.out.println("_____________________________________________");
		System.out.println("\nRemaining Waiters: \n");
			for(int i=0 ; i<4; i++){	
			 System.out.println("Waiter "+waiter[i]+ ": (" +n[i]+")	  Income: $"+ (a[i]+(a[i]*tip)));
		}
		//JOptionPane Method for Choices
		System.out.println("\nPress [ADD] \tto add waiter\nPress [PRINT] \tto print your current progress\nPress [END] \tto terminate the program. ");
		Object[] options = { "ADD", "PRINT","END"};
				int choice = JOptionPane.showOptionDialog(null,"Please select an option\n [ADD]-to add another waiter\n [PRINT]-to print your current progress\n [END]-to terminate the program","What now?",JOptionPane.YES_NO_OPTION,
				JOptionPane.QUESTION_MESSAGE,null,options,options[0]);
	
	//ADDING OF NEW WAITER
	if (choice == JOptionPane.YES_OPTION){
		
				 
				 
		System.out.println("_____________________________________________");
		System.out.println("_____________________________________________");
				 System.out.println("Enter new waiter's name: ");
				 String nname = scan.next();
				 
				 int newinc = rand.nextInt(5000)+1000;
				 //New Waiters Ranking with newly Added waiter
				 System.out.println("\nThe New Waiter's list\n");
				 for (int i=0; i<4; i++){
					 System.out.println("Waiter "+waiter[i]+ ": (" +n[i]+")	  Income: $"+ (a[i]+(a[i]*tip)));
				 }
				 System.out.println("Waiter "+waiter[4]+ ": (" +nname+")	  Income: $"+ (newinc+(newinc*tip)));
				 
				 
		System.out.println("_____________________________________________");
		System.out.println("_____________________________________________");
		//Create a File
				 try {
			File myObj = new File("C:\\Users\\jea\\Desktop\\Restaurant Log.txt");
			if (myObj.createNewFile()) { 
			System.out.println("File created: " + myObj.getName()); 
			} 
			else { System.out.println("File already exists."); }
		} 
		catch (IOException e) { 
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
		//File Writer
		try {
			FileWriter myWriter = new FileWriter("C:\\Users\\jea\\Desktop\\Restaurant Log.txt");
			myWriter.write(" RESTAURANT STAFF STATUS MANAGEMENT SYSTEM \n");
			myWriter.write(" SYSTEM LOG n");
			myWriter.write("\nRanking of Waiters According to Income: "); 
			for(int i = 0; i < 5; i++) { 
			myWriter.write("\nWaiter "+waiter[i]+ ": (" +n[i]+")	  Income: "+ a[i]); 
			}
			myWriter.write(""); 
			
			
		myWriter.write("\n_____________________________________________\n");
		myWriter.write("\nWaiter for Promotion: \n\nWaiter "+waiter[4]+ ": (" +n[4]+")	  Income: $"+(a[4]+(a[4]*tip)));
		myWriter.write("\nDate of promotion: "+Months.format(date));
		myWriter.write("\n_____________________________________________\n");
		
		//REMAINING WAITERS ON THE LIST
		myWriter.write("\n_____________________________________________\n");
		myWriter.write("\nSorted Income in Ascending Order: (UPDATED)\n");
			for(int i=0 ; i<4; i++){	
			myWriter.write("Waiter "+waiter[i]+ ": (" +n[i]+")	  Income: $"+ (a[i]+(a[i]*tip)));
		}
			myWriter.write("Waiter "+waiter[4]+ ": (" +n[4]+")	  Income: $"+ (newinc+(newinc*tip)));
			myWriter.write("\n");
			myWriter.write("---------------------------------------------\n");
			myWriter.write("R.S.S.M. Created by V.Amandoron, G.Chiong, J.Tabequero\n");
			myWriter.write("---------------------------------------------\n");
			myWriter.write("Programming 2 Class of Block CCD\n");
			myWriter.write("---------------------------------------------\n");
			myWriter.write("School Year 2019-2020 2nd Semester\n");
			myWriter.write("---------------------------------------------\n");
			myWriter.write("Final Project\n");
			myWriter.write("---------------------------------------------\n");
			myWriter.write("All rights reserved. 2020 ");
			myWriter.close();
		} 
		catch (IOException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
			
				 
	}
		else if(choice == JOptionPane.NO_OPTION){
				 System.out.println("PROGRAM TERMINATED.");
				 		 try {
			File myObj = new File("C:\\Users\\jea\\Desktop\\Restaurant Log.txt");
			if (myObj.createNewFile()) { 
			System.out.println("File created: " + myObj.getName()); 
			} 
			else { System.out.println("File already exists."); }
		} 
		catch (IOException e) { 
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
		
		try {
			FileWriter myWriter = new FileWriter("C:\\Users\\jea\\Desktop\\Restaurant Log.txt");
			myWriter.write(" RESTAURANT STAFF STATUS MANAGEMENT SYSTEM \n");
			myWriter.write(" SYSTEM LOG n");
			myWriter.write("\nRanking of Waiters According to Income: "); 
			for(int i = 0; i < 5; i++) { 
			myWriter.write("\nWaiter "+waiter[i]+ ": (" +n[i]+")	  Income: "+ a[i]);  
			}
			myWriter.write("_____________________________________________\n");
			myWriter.write("\nRanking of Waiters According to Income with 10% tip of their income:\n");
			for(int i=0 ; i<5; i++){
			 System.out.println("Waiter "+waiter[i]+ ": (" +n[i]+")	  Income: $"+ (a[i]+(a[i]*tip)));
			}
			myWriter.write(""); 
			
			
		myWriter.write("\n_____________________________________________\n");
		myWriter.write("\nWaiter for Promotion: \n\nWaiter "+waiter[4]+ ": (" +n[4]+")	  Income: $"+(a[4]+(a[4]*tip)));
		myWriter.write("\nDate of promotion: "+Months.format(date));
		myWriter.write("\n");
		myWriter.write("---------------------------------------------\n");
		myWriter.write("R.S.S.M. Created by V.Amandoron, G.Chiong, J.Tabequero\n");
		myWriter.write("---------------------------------------------\n");
		myWriter.write("Programming 2 Class of Block CCD\n");
		myWriter.write("---------------------------------------------\n");
		myWriter.write("School Year 2019-2020 2nd Semester\n");
		myWriter.write("---------------------------------------------\n");
		myWriter.write("Final Project\n");
		myWriter.write("---------------------------------------------\n");
		myWriter.write("All rights reserved. 2020 ");
		
		//REMAINING WAITERS ON THE LIST
	
			myWriter.close();
		} 
		catch (IOException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
		}
		else if(choice == JOptionPane.CANCEL_OPTION){
			System.out.println("Program has been Terminated");
		}		
		else{
					System.out.println("Please Select on the Options only");
				}
		
	
		return a;
	}
	
	
	
	
		

}


	 