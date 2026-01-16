package edu.ttu.cs.se.applogic;

import edu.ttu.cs.se.entity.InventoryEntity;
import edu.ttu.cs.se.entity.ItemEntity;
import edu.ttu.cs.se.entity.OrderEntity;
import edu.ttu.cs.se.entity.TransactionLog;
import edu.ttu.cs.se.systeminterface.BankInterface;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class CheckoutLogic {
    final static Double TAX_RATE = 0.4;
    private static OrderEntity currentOrder = null;
    private static StringBuilder receipt;
    private static String lastFourDigits = null;
    private static ArrayList<String> employeeIDs = null;
    private static final String EIN_DATA_NAME = "employeeID.csv";
    private static Boolean isAuthorized;
    private static Boolean paidByCard;

    public CheckoutLogic() {

    }

    public static void initializeLogic() {
        receipt = new StringBuilder();
        isAuthorized = false;
        paidByCard = false;
        populateEIN();
    }

    private static void populateEIN() {
        String line;
        String separator = ",";
        String cwd = System.getProperty("user.dir");
        String osFiSep= System.getProperty("file.separator");
        String file_loc = cwd + osFiSep + "src" + osFiSep + "edu" + osFiSep + "ttu"
                + osFiSep + "cs" + osFiSep + "se" + osFiSep + "data" + osFiSep + EIN_DATA_NAME;

        employeeIDs = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(file_loc))) {

            while ((line = br.readLine()) != null) {

                String[] info = line.split(separator);
                if(!info[0].equals("employeeID")) {
                    // Populate the list with parsed input
                    employeeIDs.add(info[0]);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createOrder() {
        currentOrder = new OrderEntity();
    }

    public static String getOrderContent() {
        return currentOrder.toString();
    }

    public static Boolean addItem(String item) {
        return currentOrder.addItem(InventoryEntity.getItem(item),isAuthorized);
    }

    public static Double getSubTotal() {
        return currentOrder.getSubtotal();
    }

    public static Double getOrderTotal() {
        return TAX_RATE * currentOrder.getSubtotal() + currentOrder.getSubtotal();
    }

    public static Boolean payByCash(Double amount) {
        return amount.equals(getOrderTotal());
    }

    public static Boolean payByCredit(Integer cardNum) {
        if (BankInterface.authorizeCredit(cardNum) == -1) {
            return false;
        }
        paidByCard = true;
        String cardAsString = String.valueOf(cardNum);
        setLastFourDigits(cardAsString.substring(cardAsString.length() - 4));
        return true;
    }

    public static Boolean payByDebit(Integer cardNum, Integer pin) {
        if (BankInterface.authorizeDebit(cardNum, pin) == -1) {
            return false;
        }
        paidByCard = true;
        String cardAsString = String.valueOf(cardNum);
        setLastFourDigits(cardAsString.substring(cardAsString.length() - 4));
        return true;
    }

    private static void setLastFourDigits(String str) {
        lastFourDigits = str;
    }

    public static void flushOrder() {
        currentOrder.flush();
    }

    public static String processOrder() {
        for (ItemEntity it: currentOrder) {
            InventoryEntity.removeItems(it.getName(),1);
        }

        receipt.append("RECEIPT - NOW THAT WE'VE GOT YOUR MONEY PLEASE LEAVE.");
        for (int i = 0; i < OrderEntity.ROW_LENGTH; i++) {
            receipt.append("-");
        }
        receipt.append("\n");
        receipt.append(currentOrder.toString());

        if(paidByCard) {
            receipt.append("LAST FOUR DIGITS OF CARD: " + lastFourDigits);
        }

        TransactionLog.incTransactions();
        TransactionLog.addRevenue(getOrderTotal());

        String output = receipt.toString();

        lastFourDigits = null;
        receipt.setLength(0);

        return output;
    }

    public static Boolean verifyEmployeeID(String ein) {
        isAuthorized = employeeIDs.contains(ein);
        return isAuthorized;
    }
}