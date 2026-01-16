package com.company;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        try {
            String location = "file.csv";
            File file = new File(location);
            FileWriter writer = new FileWriter(file, true);
            writer.write("cs1234,Java, someone\n");
            writer.close();
            Scanner input = new Scanner(file);
//            String line = input.nextLine();
//            String text = "";
            ArrayList<Course> data = new ArrayList<>();
            while(input.hasNext()){
//                text += input.nextLine();
                String[] line = input.nextLine().split(",");
                data.add(new Course(line[0], line[1], line[2]));
//                System.out.println(line[0]);
//                data.add(new Course(line[0]));
//               data.add(input.nextLine());
            }
            for(Course course : data){
                System.out.format("%-15s | %-35s | %-25s \n",
                        course.getCode(),course.getName(),course.getInstructor());
            }
            System.out.println(data);

//            for(String line : data){
//                line += input.nextLine();
//                System.out.println(line);
//            }
//            System.out.println(data);

        } catch (FileNotFoundException e) {
            System.out.println("error: File not found in location main");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}