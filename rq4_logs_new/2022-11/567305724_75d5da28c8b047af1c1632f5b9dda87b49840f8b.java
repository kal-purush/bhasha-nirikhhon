package manager;

import java.math.BigDecimal;

/**
 * @author Dominik_Janiga
 */
class Account {

    private double balance;

    void addIncome(double income) {
        this.balance += income;
    }

    void addPurchase(double purchase) {
        this.balance -= purchase;
    }

    public void printBalance() {
        System.out.printf("Balance:  $%.2f%n%n", this.balance);
    }

    public boolean canPurchase(Expense expense) {
        return this.balance > expense.getAmount();
    }
}