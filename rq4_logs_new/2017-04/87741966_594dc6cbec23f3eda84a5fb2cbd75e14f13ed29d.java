import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Scanner;

/**
 * Created by Adam on 4/4/17.
 */

public class mac_conversion {
    private static final String[] months = {"Jan","Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sept", "Oct", "Nov", "Dec"};

    public static void main(String[] args) {
        if(args.length != 0){
            String hex = getString(args);
            if(args[0].equalsIgnoreCase("-D")){
                date(hex);
            }else if(args[0].equalsIgnoreCase("-T")){
                time(hex);
            }else{
                System.out.println("Error. Incorrect input.");
            }
            
        }
    }
    private static String readFromFile(String file){
        String hex;
        try {
            FileReader fileReader = new FileReader(file);
            Scanner scanner =  new Scanner(fileReader);
            hex = scanner.nextLine();
            scanner.close();
            return hex;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }
    private static String getString(String[] args){
        String hex;
        if (args[1].equalsIgnoreCase("-f")) {
            hex = readFromFile(args[2]);
        }else if (args[1].equalsIgnoreCase("-h")){
            hex = args[2];
        }else {
            System.out.println("Error. Incorrect input.");
            return null;
        }

        if (hex == null) {
            return hex;
        }
        hex = hex.replace("0x", "");
        String littleEndian1, littleEndian2;
        littleEndian1 = hex.substring(0,2);
        littleEndian2 = hex.substring(2);
        hex = littleEndian2.concat(littleEndian1);
        System.out.println("le1: "+littleEndian1
                +"\nle2: "+ littleEndian2
                + "\nhex: " + hex);
        return hex;
    }

    private static void date(String hex){
        // Convert Hex String to a Integer value
        int dateInt =Integer.parseInt(hex, 16);

        // Convert Integer to Binary String
        String dateBinary = Integer.toBinaryString(dateInt);

        // Add 0 to binary string if missing beginning zeros
        for (int ii = dateBinary.length(); ii < 16; ii++) {
            dateBinary = "0" + dateBinary;
        }

        // Separate dateBinary into year, month, and day binary
        String year, month, day;
        year = dateBinary.substring(0, 7);
        month = dateBinary.substring(7, 11);
        day = dateBinary.substring(11);

        // Convert binary year, month, and day into a int values
        int y, m, d;
        y = Integer.parseInt(year, 2) + 1980; //Start year is 1980
        m = Integer.parseInt(month, 2);
        d = Integer.parseInt(day, 2);
        System.out.println("Date: " + months[m-1] + " " + d + ", " + y);
    }

    private static void time(String hex){
        // Convert Hex String to a Integer value
        int timeInt =Integer.parseInt(hex, 16);

        // Convert Integer to Binary String
        String timeBinary = Integer.toBinaryString(timeInt);

        // Add 0 to binary string if missing beginning zeros
        for (int ii = timeBinary.length(); ii < 16; ii++) {
            timeBinary = "0" + timeBinary;
        }
        System.out.println(timeBinary);
        // Separate timeBinary into year, month, and day binary
        String h, m,s;
        h = timeBinary.substring(0, 5);
        m = timeBinary.substring(5, 11);
        s = timeBinary.substring(11);
        System.out.println(m);
        // Convert binary year, month, and day into a int values
        int hr, mins, secs;
        hr = Integer.parseInt(h, 2); //Start year is 1980
        mins = Integer.parseInt(m, 2);
        secs = Integer.parseInt(s, 2)*2;
        if (hr > 12) {
            System.out.println("Time: " + (hr-12) + ":" + mins + ":" +secs + " PM");
        }else{
            System.out.println("Time: " + hr + ":" + mins + ":" +secs + " AM");
        }

    }

}