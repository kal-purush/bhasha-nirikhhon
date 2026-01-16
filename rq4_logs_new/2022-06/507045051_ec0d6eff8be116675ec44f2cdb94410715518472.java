package controller;

import model.Account;
import model.Deposit;
import model.Person;
import model.Transaction;
import repasitory.Account_tbl;
import repasitory.ConnectionManager;
import repasitory.Person_tbl;
import repasitory.Transaction_tbl;
import utility.UserInput;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ATM {
    ArrayList<Account> accounts = new ArrayList<>();

    ConnectionManager cm = new ConnectionManager();
    Connection conn = cm.connect();


    public ATM() {
    }


    // log in or signup

    public void signUp() {

        System.out.println("pls signup");
        //personal info
        System.out.println("set name");
        String name = (String) UserInput.Input.getInput(String.class);
        System.out.println("set family");
        String family = (String) UserInput.Input.getInput(String.class);
        System.out.println("set national code");
        String nCode = (String) UserInput.Input.getInput(String.class);

        //user pass
        System.out.println("set username");
        String username = (String) UserInput.Input.getInput(String.class);
        ;
        System.out.println("set password");
        String pass = (String) UserInput.Input.getInput(String.class);
        ;
        //balance
        System.out.println("set balance");
        double balance = (double) UserInput.Input.getInput(Double.class);

        this.accounts.add(new Account(username, pass, new Person(name, family, "1700080452", 25)));
        System.out.println("signup completed\n");
    }

    public void menu() {

        System.out.println(
                """    
                        [1] view my balance
                        [2] withdraw cash
                        [3] deposit founds
                        [4] show my transactions
                        [5] exit""");
    }


    public void runAtm() {
        ConnectionManager cm = new ConnectionManager();
        Connection connection = cm.connect();

        //person
        Person_tbl pt = new Person_tbl(connection);
        Person person1 = new Person();
       /* person1.setName("hamed2");
        person1.setFamily("bidar2");
        person1.setNationalCode("1234567890");
        person1.setAge(29);
         long person_tbl_ID = pt.insertPerson(person1);
         person1.setId(person_tbl_ID);*/

        //account
        Account_tbl at = new Account_tbl(connection);
        Account account1 = new Account();
       /* account1.setUsername("h2");
        account1.setPassword("123");
        account1.setBalance(200);
        account1.setPerson(person1);
        long account_tbl_ID = at.insertAccount(account1);
       account1.setId(account_tbl_ID);*/

        List<Account> accountList = at.getAllAccounts();
        System.out.println("account  list: " + accountList);

        // transaction


        Transaction_tbl tb = new Transaction_tbl(connection);
        Transaction transaction = new Deposit();
        transaction.setAccount(account1);

        //transaction.operator(account1, 200);
        //or
        //long trans_table_ID = tb.insertTransaction(700d, account1);
        //transaction.setId(trans_table_ID);

        //Transaction withdraw1 = tb.getTransactionById(17);
        //System.out.println(withdraw1);

        //List<Transaction> transactionList = tb.getAllTransactions();
        //System.out.println("transaction  list: " + transactionList);

    }

    public void logIn() throws SQLException {
        Account_tbl account_tbl = new Account_tbl(conn);
        Transaction_tbl transaction_tbl = new Transaction_tbl(conn);

        while (true) {

            System.out.println("enter username: ");
            String username = (String) UserInput.Input.getInput(String.class);
            System.out.println("enter password: ");
            String password = (String) UserInput.Input.getInput(String.class);

            Optional<Account> optionalAccount = Optional.ofNullable(account_tbl.find(username, password));


            if (optionalAccount.isPresent()) {
                Account fundedAccount = optionalAccount.get();
                System.out.println("welcome");
                ol:
                while (true) {

                    menu();
                    while (true) {
                        Integer selectMenu = (Integer) UserInput.Input.getInput(Integer.class);

                        if (selectMenu == 1) {
                            System.out.println(fundedAccount.getBalance());
                            break;
                        } else if (selectMenu == 2) {
                            System.out.println("how many u wanna withdraw");
                            double money = (Double) UserInput.Input.getInput(Double.class);
                            fundedAccount.withdrawAction(money);
                            double currentBalance= fundedAccount.getBalance();
                            account_tbl.updateBalance(fundedAccount);
                            System.out.println("new balance" + currentBalance);
                            transaction_tbl.insertTransaction(money, fundedAccount);
                            break;
                        } else if (selectMenu == 3) {
                            System.out.println("how many u wanna deposit");
                            double money = (Double) UserInput.Input.getInput(Double.class);
                            fundedAccount.depositAction(money);
                            double currentBalance= fundedAccount.getBalance();
                            account_tbl.updateBalance(fundedAccount);
                            System.out.println("new balance" + currentBalance);
                            transaction_tbl.insertTransaction(money, fundedAccount);
                            break;
                        } else if (selectMenu == 4) {
                            System.out.println("transactions: ");
                            System.out.println(transaction_tbl.getAllTransactions());
                            break;
                        } else if (selectMenu == 5)
                            break ol;
                    }
                }
            } else {
                System.err.println("wrong user or pass!");
                // throw new InvalidInputException(Message.INVALID_USERNAME_OR_PASS);
            }
        }
    }
}