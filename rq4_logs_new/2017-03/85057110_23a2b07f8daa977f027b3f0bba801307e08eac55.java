package Ferrari;

import Ferrari.models.Car;
import Ferrari.models.Ferrari;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by r3v3nan7 on 15.03.17.
 */
public class Main {

    public static void main(String[] args) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        String driverName = reader.readLine();

        Car ferrari = new Ferrari(driverName);
        System.out.println(ferrari.toString());
    }

}