package controllers;

import models.library.content.Item;
import models.people.Person;
import models.library.management.Loan;
import models.library.management.Reserve;
import models.library.management.Library;
import java.util.HashSet;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map;

public class LibraryController {
    // Private Attributes ---------------------------------------------------------------
    private final HashMap<String, Item> items;
    private final HashSet<Loan> activeLoans;
    private final ArrayList<Reserve> reserves;

    // Constructor ----------------------------------------------------------------------
    public LibraryController() {
        this.items = new HashMap<>();
        this.activeLoans = new HashSet<>();
        this.reserves = new ArrayList<>();
    }

    // Methods --------------------------------------------------------------------------

    /**
     * @return item map.
     */
    public HashMap<String, Item> listItems() {
        return items;
    }

    /**
     * @return active loans set.
     */
    public HashSet<Loan> listLoans() {
        return activeLoans;
    }

    /**
     * @return reserve list.
     */
    public ArrayList<Reserve> listReserves() {
        return reserves;
    }

    /**
     * Adds an item to the items map.
     *
     * @param item item to add.
     */
    public void addItem(Item item) {
        this.items.put(item.getId(), item);
    }

    /**
     * Adds a loan to the active loans set.
     *
     * @param loan loan to add.
     */
    public void addLoan(Loan loan) {
        this.activeLoans.add(loan);
    }

    /**
     * Adds a reserve to the reserve list.
     *
     * @param reserve reserve to be added.
     */
    public void addReserve(Reserve reserve) {
        (this.reserves).add(reserve);
    }

    /**
     * Removes an item from the items map.
     *
     * @param item item to remove.
     */
    public void removeItem(Item item) {
        this.items.remove(item);
    }

    /**
     * Removes a loan from the active loans set.
     *
     * @param loan loan to be removed.
     */
    public void removeLoan(Loan loan) {
        this.activeLoans.remove(loan);
    }

    /**
     * Removes a reserve from the reserve list.
     *
     * @param reserve reserve to be removed.
     */
    public void removeReserve(Reserve reserve) {
        (this.reserves).remove(reserve);
    }

    /**
     * Tries to loan an item to a user.
     *
     * @param user user trying to perform the loan.
     * @param item to loan.
     * @return true on success and false on failure.
     */
    public boolean lendItem(Item item, Person user) {
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

    /**
     * Searches for an Item by its ID.
     *
     * @param id the item's ID.
     * @return the item with the corresponding ID.
     */
    public Item searchItemById(String id) {
        return items.get(id);
    }
}