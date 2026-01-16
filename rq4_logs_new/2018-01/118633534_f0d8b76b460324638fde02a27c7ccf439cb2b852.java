/**
 * CSCI 4020
 * Dr. Nicholson
 * William Kersten
 * 1/29/2018
 */

public class Wallet {
    double cash;
    double maxCashHeld;
    double totalEarnings;
    double totalLost;

    /**
     * Creates a Wallet object with $0.
     */
    public Wallet() {
        cash = 0;
        //in future builds, possibly keep this in a file and grab it back again?
        maxCashHeld = 0;
        totalEarnings = 0;
        totalLost = 0;
    }

    /**
     * creates a Wallet object with an input amount of cash.
     * @param cash how much cash to start the Wallet with.
     */
    public Wallet(double cash){
        this.cash = cash;
        maxCashHeld = 0;
        totalEarnings = 0;
        totalLost = 0;
    }

    /**
     * @return the amount of cash in the wallet, preceded by a $.
     */
    @Override
    public String toString() {
        return "$" + cash;
    }

    public double getCash() {
        return cash;
    }

    /**
     * Adds an input amount of cash to the wallet, updating totalEarnings and maxCashHeld
     * in the process.
     * @param cash the amount of money to add to the wallet.
     */
    public void addCash(double cash) {
        this.cash += cash;
        this.totalEarnings += cash;
        if (this.cash > maxCashHeld) {
            maxCashHeld = this.cash;
        }
    }

    /**
     * removes an input amount of cash from the wallet, updating totalLost in the process.
     * @param cash the amount of money to remove from the wallet.
     */
    public void removeCash(double cash) {
        this.cash -= cash;
        this.totalLost += cash;
    }

    public double getMaxCashHeld() {
        return maxCashHeld;
    }

    public double getTotalEarnings() {
        return totalEarnings;
    }

    public double getTotalLost() {
        return totalLost;
    }

}