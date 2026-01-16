package codedemo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import codedemo.Util.InputUtil;

/**
 * This class is the main class of the application.
 * It prompts the user for input and displays the output
 * Specifically, it prompts the user for the tool code, checkout date, rental days, and discount
 * It then calculates the due date, charge days, pre-discount charge, discount amount, and final charge
 */
public class App 
{
    public static void main( String[] args )
    {
        String input = "";
        ToolLoader toolLoader = new ToolLoader();

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        while (!input.equals("exit")) {
            try {
                System.out.println("Enter tool code: ");
                String toolCode = reader.readLine();
                System.out.println("Enter rental days: ");
                String rentalDays = reader.readLine();
                System.out.println("Enter checkout date: ");
                String checkoutDate = reader.readLine();
                System.out.println("Enter discount percent: ");
                String discountPercent = reader.readLine().trim();

                //We now use InputUtil to check if the user input is valid
                InputUtil inputUtil = new InputUtil();
                if (inputUtil.isDiscountValid(discountPercent)
                    && inputUtil.isNumberRentalDaysValid(rentalDays)
                    && inputUtil.isToolCodeValid(toolCode)
                    && inputUtil.isDateValid(checkoutDate)) {

                    //if the user input is valid, we create a Checkout object
                    new Checkout(toolCode, Integer.parseInt(rentalDays),
                        LocalDate.parse(checkoutDate, DateTimeFormatter.ofPattern("M/d/[yyyy][yy]")),
                        Integer.parseInt(discountPercent));
                }
                System.out.println("Enter 'exit' to quit or any other key to continue: ");
                input = reader.readLine();
            }
            catch(IOException | NumberFormatException| DateTimeParseException e ){
            e.printStackTrace();
            }
        }
    }
}