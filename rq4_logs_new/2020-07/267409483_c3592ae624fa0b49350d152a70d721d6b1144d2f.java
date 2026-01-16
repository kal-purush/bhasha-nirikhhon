/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package world.opentexts.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.io.Reader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 * Hopefully does what it says on the tin!
 * 
 * @see https://twitter.com/Chenshaw/status/1285233593355243522
 * 
 * @author Stuart Lewis
 */
public class FindLongestTitle {
    
    public static void main(String[] args) throws Exception {
        String one = "";
        String two = "";
        String three = "";
        String four = "";
        String five = "";
        
        File dir = new File("c:\\otw\\");
        File[] directoryListing = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(".csv");
            }
        });
        if (directoryListing != null) {
            for (File child : directoryListing) {
                Reader in = new BufferedReader(new InputStreamReader(new FileInputStream(child), "UTF-8"));
                CSVParser csvParser = new CSVParser(in, CSVFormat.DEFAULT
                    .withHeader("organisation",
                                "idLocal",
                                "title",
                                "urlMain",
                                "year",
                                "publisher",
                                "creator",
                                "topic",
                                "description",
                                "urlPDF",
                                "urlOther",
                                "urlIIIF",
                                "placeOfPublication",
                                "licence",
                                "idOther",
                                "catLink",
                                "language")
                    .withIgnoreHeaderCase()
                    .withTrim());
                boolean changed = false;
                for (CSVRecord record : csvParser) {
                    String title = record.get("title");
                    if (title.length() >= one.length()) {
                        five = four;
                        four = three;
                        three = two;
                        two = one;
                        one = title;
                        changed = true;
                    } else if (title.length() >= two.length()) {
                        five = four;
                        four = three;
                        three = two;
                        two = title;
                        changed = true;
                    } else if (title.length() >= three.length()) {
                        five = four;
                        four = three;
                        three = title;
                        changed = true;
                    } else if (title.length() >= four.length()) {
                        five = four;
                        four = title;
                        changed = true;
                    } else if (title.length() >= five.length()) {
                        five = title;
                        changed = true;
                    } 
                    
                    if (changed) {
                        System.out.println("1) (" + one.length() + ") " + one + "\n" +
                                           "2) (" + two.length() + ") " + two + "\n" +
                                           "3) (" + three.length() + ") " + three + "\n" +
                                           "4) (" + four.length() + ") " + four + "\n" +
                                           "5) (" + five.length() + ") " + five + "\n");
                        changed = false;
                    }
                }
            }
        }
    }
}