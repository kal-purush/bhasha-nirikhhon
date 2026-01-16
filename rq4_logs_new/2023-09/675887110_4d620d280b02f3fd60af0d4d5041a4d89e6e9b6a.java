package controllers;

import models.library.content.Item;
import models.people.Person;
import models.library.management.Loan;
import models.library.management.Library;
import java.util.ArrayList;

public class LibraryController {
    // Private Attributes ---------------------------------------------------------------
    private final ArrayList<Item> items;
    private final ArrayList<Loan> activeLoans;

    // Constructor ----------------------------------------------------------------------
    public LibraryController() {
        this.items = new ArrayList<>();
        this.activeLoans = new ArrayList<>();
    }

    // Methods --------------------------------------------------------------------------

    /**
     * @return item list.
     */
    public ArrayList<Item> listItems() {
        return items;
    }

    /**
     * @return active loans list.
     */
    public ArrayList<Loan> listLoans() {
        return activeLoans;
    }

    /**
     * Adds an item to the items list.
     *
     * @param item item to add.
     */
    public void addItem(Item item) {
        this.items.add(item);
    }

    /**
     * Adds a loan to the active loans list.
     *
     * @param loan loan to add.
     */
    public void addLoan(Loan loan) {
        this.activeLoans.add(loan);
    }

    /**
     * Removes an item from the items list.
     *
     * @param item item to remove.
     */
    public void removeItem(Item item) {
        this.items.remove(item);
    }

    /**
     * Removes a loan from the active loans list.
     *
     * @param loan loan to be removed.
     */
    public void removeLoan(Loan loan) {
        this.activeLoans.remove(loan);
    }

    /**
     * Tries to loan an item to a user.
     *
     * @param user user trying to perform the loan.
     * @param item to loan.
     * @return true on success and false on failure.
     */
    public boolean lendItem(Library library, Item item, Person user) {
        if (!user.getIsSuspended() && item.getAvailable()) {
            // Loan logic will go here.
            return true;

        }
        else {
            return false;
        }
    }

    /**
     * Retrieves an item from a user and closes the loan.
     *
     * @param user user who currently holds the item.
     * @param item item to retrieve.
     */
    public void retrieveItem(Person user, Item item) {
        // Retrieval logic will go here.
    }
}