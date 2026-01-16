package Telephony;

import Telephony.models.Smartphone;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by r3v3nan7 on 15.03.17.
 */
public class Main {

    public static void main(String[] args) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        Smartphone smartphone = new Smartphone();

        String[] numbersToCal = reader.readLine().split(" ");
        String[] sitesForBrowsing = reader.readLine().split(" ");

        for (String number : numbersToCal) {
            System.out.println(smartphone.calling(number));
        }


        for (String site : sitesForBrowsing) {
            System.out.println(smartphone.browsing(site));
        }


    }

}