
class Account {
    long id;
    double balance;


    public Account(long id, String balance) {
        this.id = id;
        this.balance = Double.parseDouble(balance.replace("$", ""));
    }

    void withdraw(double amount) throws Exception {
        if (balance >= amount) {
            balance -= amount;
        } else {
            throw new Exception();
        }
    }

    void deposit(double amount) {
        balance += amount;
    }
}

abstract class Transaction {
    protected final long fromId;
    protected final long toId;
    protected final String description;
    protected final double amount;

    public Transaction(long fromId, long toId, String description, String amount) {
        this.fromId = fromId;
        this.toId = toId;
        this.description = description;
        this.amount = Double.parseDouble(amount.replace("$", ""));
    }

    abstract double getProvision();

    double getTotalAmount() {
        return amount + getProvision();
    }
}

class FlatAmountProvisionTransaction extends Transaction {

    double provision;

    public FlatAmountProvisionTransaction(long fromId, long toId, String description, String amount, String flatProvision) {
        super(fromId, toId, description, amount);
        this.provision = Double.parseDouble(flatProvision.replace("$", ""));
    }

    @Override
    double getProvision() {
        return this.provision;
    }
}

class FlatPercentProvisionTransaction extends Transaction {

    int centsPerDollar;

    public FlatPercentProvisionTransaction(long fromId, long toId, String description, String amount, int centsPerDollar) {
        super(fromId, toId, description, amount);
        this.centsPerDollar = centsPerDollar;
    }

    @Override
    double getProvision() {
        //21.5$
        return (int) amount * centsPerDollar / 100.0;
    }
}

class Bank {
    String name;
    Account[] accounts;

    public Bank(String name, Account[] account) {
        this.name = name;
        this.accounts = account;
    }

    Account findAccount(long id) {
        for (Account account : accounts) {
            if (account.id == id) {
                return account;
            }
        }
        return null;
    }

    boolean makeTransaction(Transaction t) {
        Account from = findAccount(t.fromId);
        Account to = findAccount(t.toId);

        if (from == null || to == null) {
            return false;
        }

        try {
            from.withdraw(t.getTotalAmount());
            to.deposit(t.amount);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}

public class BankTester {

}