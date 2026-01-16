package cooper.storage;

import cooper.exceptions.InvalidFileDataException;
import cooper.finance.FinanceManager;
import cooper.ui.Ui;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Scanner;

public class BalanceSheetStorage extends Storage {

    public BalanceSheetStorage(String filePath) {
        super(filePath);
    }

    public void loadBalanceSheet(FinanceManager cooperFinanceManager) {
        ArrayList<Integer> balanceSheet = cooperFinanceManager.getBalanceSheet();
        Scanner fileScanner = getScanner(filePath);
        readBalanceSheet(fileScanner, balanceSheet);
    }

    public void saveBalanceSheet(FinanceManager cooperFinanceManager) {
        try {
            writeBalanceSheet(filePath, cooperFinanceManager.getBalanceSheet());
        } catch (IOException e) {
            System.out.println("Error writing to file: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void readBalanceSheet(Scanner fileScanner, ArrayList<Integer> balanceSheet) {
        if (fileScanner != null) {
            while (fileScanner.hasNext()) {
                String expense = fileScanner.nextLine();
                try {
                    int decodedExpense = decodeExpense(expense);
                    balanceSheet.add(decodedExpense);
                } catch (InvalidFileDataException e) {
                    Ui.showInvalidFileDataError();
                }
            }
        }
    }

    private static int decodeExpense(String expense) throws InvalidFileDataException {
        if (isInvalidFileData(expense)) {
            throw new InvalidFileDataException();
        }
        return Integer.parseInt(expense);
    }

    private static boolean isInvalidFileData(String expenseAsString) {
        try {
            int expense = Integer.parseInt(expenseAsString);
        } catch (NumberFormatException e) {
            return true;
        }
        return false;
    }

    private static void writeBalanceSheet(Path filePath, ArrayList<Integer> balanceSheet) throws IOException {
        FileWriter fileWriter = new FileWriter(filePath.toString(), false);

        for (Integer expense : balanceSheet) {
            String encodedExpense = encodeExpense(expense);
            fileWriter.write(encodedExpense + System.lineSeparator());
        }
        fileWriter.close();
    }

    private static String encodeExpense(Integer expense) {
        return expense.toString();
    }
}