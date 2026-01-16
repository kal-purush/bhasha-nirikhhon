/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hospitalrecords;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Scanner;

/**
 *
 * @author 420190038
 */
public class HospitalRecords {
    
    private static ArrayList objectList = new ArrayList();
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        
        HospitalRecords.obtainRecords();
        Scanner sc = new Scanner(System.in);
        System.out.println("**Patient records have been recorded successfully**\nPress S for search, Press E for exit.");
        
        while (true) {
            String input = sc.next();
            if ("S".equals(input.toUpperCase())) {
                System.out.println("\nEnter Patient ID to search. Please use capitals.");
                int a = 0;
                input = sc.next();
                if (input.length() == 6) {
                    for (Object id : objectList) {
                        if (id.toString().contains(input)){ 
                            System.out.println("\n" + id + "\nWould you like to delete this patients records? Y or N.");
                            String input2 = sc.next();
                            a++;
                            if ("Y".equals(input2.toUpperCase())){
                                objectList.remove(id);
                                System.out.println("\nDeletion successful.");
                                break;
                            }else if ("N".equals(input2.toUpperCase())) {
                                System.out.println("\nNo Deletion done.");
                            }else {
                                System.out.println("\nY or N please.");
                            }
                        }
                    }   
                } else {
                }
                if (a > 0) {
                    System.out.println("\nOverall Success. To continue enter S or E.");
                }else {
                    System.out.println("\nNo such ID found. To continue enter S or E.");
                }
            }else if ("E".equals(input.toUpperCase())) {
                return; // exit application
            }else {
                System.out.println("\nPlease enter S or E.");
            }
        }
    }
    
    public static void printList(ArrayList list){
        list.forEach((o) -> {
            System.out.println(o + "\n");
        });
    }
    
    public static void obtainRecords(){
        String path = "ListOfPatients.txt";
        //Use path to the ListOfPatients.txt here
        LineNumberReader lr = null;
        //allows to read information from the file line by line
        String[] oneRecord = new String[4];
        //an array to store info about one patient
        Patient pat;
        try {
            //using try-catch for exception handling to catch
            //possible errors with i/o operations
            FileReader inputStream = new FileReader(path);
            //FileReader reads the file’s contents
            lr = new LineNumberReader(inputStream);
            String str;
            while((str = lr.readLine())!= null) {
                //while the next line exists
                oneRecord = str.split(",");
                //dividing one file line by commas and assigning to array
                pat = new Patient(oneRecord[0], oneRecord[1],
                oneRecord[2], oneRecord[3]);
                    objectList.add(pat); //adding patient to the ArrayList
                }
            }
        catch(IOException ioe){
            System.out.println("IOExcception occured" + ioe);
        }
    }
}