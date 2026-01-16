package models.library.management;

import java.util.ArrayList;

public class LoanList<T extends Loan> {
    // Private Attributes ---------------------------------------------------------------
    private final ArrayList<T> loans;
    private int numOfLoans;

    // Constructor ----------------------------------------------------------------------
    public LoanList() {
        this.loans = new ArrayList<>();
        this.numOfLoans = 0;
    }

    // Getters --------------------------------------------------------------------------
    public ArrayList<T> getLoans() {
        return loans;
    }

    public int getNumOfLoans() {
        return numOfLoans;
    }

    // Setters --------------------------------------------------------------------------
    public void setNumOfLoans(int numOfLoans) {
        this.numOfLoans = numOfLoans;
    }

    // Methods --------------------------------------------------------------------------
    public void addLoan(T loan) {
        (this.loans).add(loan);
    }
}