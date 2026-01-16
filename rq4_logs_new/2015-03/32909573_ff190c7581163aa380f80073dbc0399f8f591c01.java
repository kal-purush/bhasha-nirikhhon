/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.SortedSet;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author student
 */
public class NewMain {

    /**
     * @param args the command line arguments
     */

    public static void main(String[] args) {
        // TODO code application logic here

        try {
                       BufferedReader br=new BufferedReader(new FileReader("/home/student/NetBeansProjects/test/build/classes/test/nouns/91Knouns.txt"));

BufferedReader br1=new BufferedReader(new FileReader("/home/student/NetBeansProjects/test/build/classes/test/problem.txt"));

        BufferedWriter bw=new BufferedWriter(new FileWriter("/home/student/Documents/untitledfolder/noun.txt"));

            HashSet<String> sub =new HashSet<String>();
            HashSet<String> fn =new HashSet<String>();
            String s;
             System.out.println("starting");
            while((s=br.readLine())!=null)
            {
               sub.add(s);
            }

            while((s=br1.readLine())!=null)
            {
               String token[] = s.split(" ");
               for(int i=0;i<token.length;i++)
               {
                   System.out.println(token[i]);
                   if(sub.contains(token[i]))
                   {
                       System.out.println(token[i]);
                       if(fn.add(token[i]))
                       bw.append(token[i]+"\n");
                   }

               }
            }
            br.close();
            br1.close();
            bw.close();
             System.out.println(sub.size());

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}