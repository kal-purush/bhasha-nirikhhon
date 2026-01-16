package com;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

abstract public class BankManager {
    private static Customer selectedCustomer, selectedCustomerTwo;
    private static Account selectedAccount, selectedAccountTwo;
    private static String selectedCustomerPath, selectedAccountPath, selectedAccountPathTwo;

    public enum FileProperty {
        FIRSTNAME,
        LASTNAME,
        EMAIL,
        BALANCE,
        DEBT
    }

    public static void buildDirectories() {
        try {
            if (!Files.exists(Paths.get("Javabank"))) Files.createDirectory(Paths.get("Javabank"));
            if (!Files.exists(Paths.get("Javabank/Customer"))) Files.createDirectory(Paths.get("Javabank/Customer"));
            if (!Files.exists(Paths.get("Javabank/Account"))) Files.createDirectory(Paths.get("Javabank/Account"));
            if (!Files.exists(Paths.get("Javabank/Personnel"))) Files.createDirectory(Paths.get("Javabank/Personnel"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void selectCustomer(List<String> customerList, int input) {
        if (input == 0) {
            PrintMenu.main();
            MenuSelection.main(Input.number("Mata in val: "));
        } else if (input > 0 && input <= customerList.size()) {
            selectedCustomerPath = customerList.get(input - 1);
            List<String> customerProperties = new ArrayList<>();
            for (String property : FileManager.readData(customerList.get(input - 1))) {
                customerProperties.add(property.split(":")[1]);
            }
            selectedCustomer = new Customer(customerProperties.get(0), customerProperties.get(1), customerProperties.get(2), Long.parseLong(customerProperties.get(3)));
            PrintMenu.customerOptions();
            MenuSelection.customerOptions(Input.number("Mata in val: "));
        }
    }

    public static void selectCustomerTwo(List<String> customerList, int input) {
        if (input == 0) {
            PrintMenu.main();
            MenuSelection.main(Input.number("Mata in val: "));
        } else if (input > 0 && input <= customerList.size()) {
            selectedCustomerPath = customerList.get(input - 1);
            List<String> customerProperties = new ArrayList<>();
            for (String property : FileManager.readData(customerList.get(input - 1))) {
                customerProperties.add(property.split(":")[1]);
            }
            selectedCustomerTwo = new Customer(customerProperties.get(0), customerProperties.get(1), customerProperties.get(2), Long.parseLong(customerProperties.get(3)));
        }
    }

    public static Customer getSelectedCustomer() {
        return selectedCustomer;
    }

    public static  Customer getSelectedCustomerTwo() {
        return selectedCustomerTwo;
    }

    public static void selectAccount(List<String> accountList, int input) {
        if (input == 0) {
            PrintMenu.customerOptions();
            MenuSelection.customerOptions(Input.number("Mata in val: "));
        } else if (input > 0 && input <= accountList.size()) {
            selectedAccountPath = accountList.get(input - 1);
            List<String> accountProperties = new ArrayList<>();
            for (String property : FileManager.readData(accountList.get(input - 1))) {
                accountProperties.add(property.split(":")[1]);
            }
            selectedAccount = new Account(Integer.parseInt(accountProperties.get(0)), Double.parseDouble(accountProperties.get(1)), Double.parseDouble(accountProperties.get(2)), Long.parseLong(accountProperties.get(3)));
            PrintMenu.accountOptions();
            MenuSelection.accountOptions(Input.number("Mata in val: "));
        }
    }

    public static void selectAccountTwo(List<String> accountList, int input) {
        if (input == 0) {
            PrintMenu.accountOptions();
            MenuSelection.accountOptions(Input.number("Mata in val: "));
        } else if (input > 0 && input <= accountList.size()) {
            selectedAccountPathTwo = accountList.get(input - 1);
            List<String> accountProperties = new ArrayList<>();
            for (String property : FileManager.readData(accountList.get(input - 1))) {
                accountProperties.add(property.split(":")[1]);
            }
            selectedAccountTwo = new Account(Integer.parseInt(accountProperties.get(0)), Double.parseDouble(accountProperties.get(1)), Double.parseDouble(accountProperties.get(2)), Long.parseLong(accountProperties.get(3)));
        }
    }

    public static Account getSelectedAccount() {
        return selectedAccount;
    }

    public static Account getSelectedAccountTwo() {
        return selectedAccountTwo;
    }

    public static void transferMoney(double transactionSum) {
        System.out.println();
        selectedAccount.setAccountBalance(selectedAccount.getAccountBalance() - transactionSum);
        FileManager.write(selectedAccountPath, selectedAccount.getList());
        selectedAccountTwo.setAccountBalance(selectedAccountTwo.getAccountBalance() + transactionSum);
        FileManager.write(selectedAccountPathTwo, selectedAccountTwo.getList());
    }

    public static void editFile(FileProperty fileProperty) {
        if (fileProperty == FileProperty.FIRSTNAME) {
            FileManager.delete(selectedCustomerPath);
            List<String> splitPath = Arrays.asList(selectedCustomerPath.split(selectedCustomer.getFirstName()));
            selectedCustomer.setFirstName(Input.string("Mata in nytt förnamn: "));
            FileManager.write(splitPath.get(0) + selectedCustomer.getFirstName() + splitPath.get(1), selectedCustomer.getList());
        }

        if (fileProperty == FileProperty.LASTNAME) {
            FileManager.delete(selectedCustomerPath);
            List<String> splitPath = Arrays.asList(selectedCustomerPath.split(selectedCustomer.getLastName()));
            selectedCustomer.setLastName(Input.string("Mata in nytt efternamn: "));
            FileManager.write(splitPath.get(0) + selectedCustomer.getLastName() + splitPath.get(1), selectedCustomer.getList());
        }

        if (fileProperty == FileProperty.EMAIL) {
            String newEmail = Input.string("Mata in ny email: ");
            while (!Validate.email(newEmail)) {
                System.out.println("#invalid email#");
                newEmail = Input.string("Mata in ny email: ");
            }
            selectedCustomer.setEmail(newEmail);
            FileManager.write(selectedCustomerPath, selectedCustomer.getList());
        }

        if (fileProperty == FileProperty.BALANCE) {
            selectedAccount.setAccountBalance(Input.floatingNumber("Mata in nytt saldo: "));
            FileManager.write(selectedAccountPath, selectedAccount.getList());
        }
        if (fileProperty == FileProperty.DEBT) {
            selectedAccount.setDebt(Input.floatingNumber("Mata in ny skuld: "));
            FileManager.write(selectedAccountPath, selectedAccount.getList());
        }
    }

    public static Customer createCustomer() {
        String firstName;
        do {
            firstName = Input.string("Mata in förnamn: ");
        } while (!Validate.name(firstName));

        String lastName;
        do {
            lastName = Input.string("Mata in efternamn: ");
        } while (!Validate.name(lastName));

        String email;
        do {
            email = Input.string("Mata in E-post adress: ");
        } while (!Validate.email(email));

        long ssn;
        do {
            ssn = Input.longNumber("Mata in personnummer: ");
        } while (String.valueOf(ssn).length() != 10 && String.valueOf(ssn).length() != 12);

        Customer customer = new Customer(firstName, lastName, email, ssn);
        FileManager.write("Javabank/Customer/" + UUID.randomUUID() + "-" + customer.getFirstName() + "_" + customer.getLastName() + ".txt", customer.getList());
        return customer;
    }

    public static Account createAccount(Customer customer, int balance, int debt) {
        Account account = new Account(AccountNumber.randomNumber(), balance, debt, customer.getSocialSecurityNumber());
        FileManager.write("Javabank/Account/" + new Date().getTime() + ".txt", account.getList());
        return account;
    }

    public static List<String> listAccounts(long ssn) {
        List<String> searchResults = new ArrayList<>();
        List<String> accountData;
        Account accountFile;
        for (Path filePath : FileManager.listFiles("Javabank/Account")) {
            accountData = new ArrayList<>();
            for (String line : FileManager.readData(filePath.toString())) {
                accountData.add(line.split(":")[1]);
            }
            accountFile = new Account(Integer.parseInt(accountData.get(0)), Double.parseDouble(accountData.get(1)), Double.parseDouble(accountData.get(2)), Long.parseLong(accountData.get(3)));
            if (accountFile.getSSN() == ssn) {
                searchResults.add(filePath.toString());
            }
        }
        return searchResults;
    }

    public static void deleteCustomer() {
        PrintMenu.main();
        MenuSelection.main(Input.number("Mata in val: "));
        FileManager.delete(selectedCustomerPath);
        long ssn = selectedCustomer.getSocialSecurityNumber();
        for (String path : listAccounts(ssn)) {
            FileManager.delete(path);
        }
    }

    public static void deleteAccount() {
        PrintMenu.customerOptions();
        MenuSelection.customerOptions(Input.number("Mata in val: "));
        FileManager.delete(selectedAccountPath);
    }
}