package ampeg;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.awt.List;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
public class Test {
	public static void main(String[] args) throws IOException {
        // open file input stream
        BufferedReader reader = new BufferedReader(new FileReader(
                "./resources/basses.csv"));
      //  List<HashMap<String, Object>> custom2 = new HashMap<String, Object>>();
        List<HashMap<String, Object>> retHashMap = new ArrayList<HashMap<String, Object>>();
        //HashMap<String, Object> itemMap = new HashMap<String, Object>();
        //<HashMap<String, Object>> retHashMap = new ArrayList<HashMap<String, Object>>();
        String line = null;
        ArrayList<String> lines = new ArrayList<String>();
       
        String[] headtilte = reader.readLine().split(",");
        String[] newArray = new String[headtilte.length-1];
        int num = 2;
        
        for(int i=0;i<newArray.length; i++) {
            // 判断元素是否越界
            if (num < 0 || num >= headtilte.length) {
                throw new RuntimeException("元素越界... "); 
            } 
            // 
            if(i<num) {
                newArray[i] = headtilte[i];
            }
            else {
                newArray[i] = headtilte[i+1];
            }
        }
          
       // System.out.println(newArray[1]);
        
        
        int w=0;
        while ((line = reader.readLine()) != null) { 
        	HashMap<String, Object> itemMap = new HashMap<String, Object>();
        	String[] itemArray = line.split(",");
      
        	//System.out.println(itemArray.length);
        	for (int i = 0; i < itemArray.length; i++) {
        		
       
        		itemMap.put(headtilte[i], itemArray[i]);
           		System.out.println(headtilte[i]+"    "+itemArray[i]);
           	
        		
            }
        	retHashMap.add(itemMap);
        } 
       // System.out.println(retHashMap);
        
       // System.out.println(lines.get(0));
        
}}
	