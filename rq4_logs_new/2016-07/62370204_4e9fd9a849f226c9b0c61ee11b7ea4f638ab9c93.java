package com.spk.coll;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

public class ArraysTester {
	
	public static void main(String[] args) {
		
		ArrayList<String> aListMonth = new ArrayList<String>();
        aListMonth.add("Feb");
        aListMonth.add("mar");
        aListMonth.add("Jan");
        
        //convert arraylist to object array
        Object[] objMnt = aListMonth.toArray();
        
        //convert obj array to string array
        String[] strMnts = Arrays.copyOf(objMnt, objMnt.length, String[].class);
        
        //print elements of String array
        for(int i=0; i < strMnts.length; i++){
                System.out.println(strMnts[i]);
        }
        System.out.println("NOTE ORDERING IS PRESERVED EVEN AFTER 3 convertions");      
                               
	}

}