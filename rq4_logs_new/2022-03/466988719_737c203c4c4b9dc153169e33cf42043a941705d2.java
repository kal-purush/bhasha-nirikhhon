package com.company;


import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class Main {


    public void printPermutn(String str, String ans)
    {
        if (str.length() == ans.length()) {
            System.out.println("Permutation : "+ans);
            return;
        }

        for (int i = 0; i < str.length(); i++) {
            printPermutn(str, ans + str.charAt(i));

        }
    
    }
    public String longestPalidrome(String a){
        int n=a.length();
        String lb = null;
        int i,j,max=0,k;
        for(i=0;i<n;i++){
            for(j=i+1;j<n;j++){
                if(a.charAt(i)==a.charAt(j)){
                    String pl=a.substring(i,j+1);
                    String sb=a.substring(i,j+1);
                    String sbrev="";
                    for(int m=sb.length()-1;m>=0;m--)
                        sbrev+=sb.charAt(m);
                    if(pl.equals(sbrev)){
                        k=pl.length();
                        if(max<k) {
                            lb = pl;
                            max=pl.length();
                        }
                    }
                }
            }
        }
        return  lb;
    }
    public void longestStringInFile() throws FileNotFoundException {
        File file = new File("C:\\Users\\HP\\Downloads\\Sample3.txt");
        int max=0;
        String op = null;
        Scanner myReader = new Scanner(file);
        while (myReader.hasNext()) {
            String data = myReader.next();
            if(data.length()>max) {
                op = data;
                max=data.length();
            }
        }
        System.out.println("Longest String In File: "+op);
    }
    public void greatestAndLeastKey(){
        Map<Integer,Integer> map=new HashMap<>();
        map.put(1,18);
        map.put(2,20);
        map.put(3,45);
        map.put(4,8);
        map.put(5,6);
        System.out.println("Maximum value key in map: "+map.entrySet().stream().max((e1,e2)-> e2.getValue().compareTo(e1 .getValue())).get().getKey());
        System.out.println("Minimum value key in map: "+map.entrySet().stream().max((e1,e2)-> e1.getValue().compareTo(e2.getValue())).get().getKey());
    }
    public static void main(String[] args) throws FileNotFoundException {

        Main main = new Main();
        main.printPermutn("pqr","");
        String lb=main.longestPalidrome("theehtquickbrownfoxxofnworbquickkciuq");
        System.out.println("Longest Palidrome in String: "+lb);
        main.greatestAndLeastKey();
        main.longestStringInFile();

    }
}