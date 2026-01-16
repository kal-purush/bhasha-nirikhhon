/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ElevatorProject;

import static java.nio.file.StandardOpenOption.*;
import java.nio.file.*;
import java.io.*;
import java.util.ArrayDeque;

/**
 *
 * @author nicholas
 */
public class Report {
    private static final ArrayDeque<Person> bufferQueue = new ArrayDeque<>();
    
    public Report(){

    }
    
    public static void addToReport(Person p){
        bufferQueue.add(p);
    }
    
    public static void emptyToFile(){
        //String s = "";
        //byte data[] = s.getBytes();
        Path p = Paths.get("./logfile.txt");

        try (OutputStream out = new BufferedOutputStream(
            Files.newOutputStream(p, CREATE, APPEND))) {
            String s;
            while(!bufferQueue.isEmpty()){
                s = bufferQueue.getFirst().toString();
                byte data[] = s.getBytes();
                out.write(data, 0, data.length);
            }
        } catch (IOException x) {
            System.err.println(x);
        }
    }
    
}