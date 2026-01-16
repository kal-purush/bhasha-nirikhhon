package models.library.management;

import models.library.content.*;
import models.library.people.*;

/**
 * Generic class that manage reserves and loans of a library belonging to an institute of the university.
 *
 * @author Matheus Reato (RA: 244088), Caio Taishi (RA: 242908).
 */

public class LibraryItem<T extends Item> {
    // Private Attributes ***********************************************************
    private final ArrayList<T> reservedItems;
    private final ArrayList<T> loanItems;
    private double numOfReservedItems;
    private double numOfLoanItems;

    // Constructor ******************************************************************
    public LibraryItem() {
        this.reservedItems = new ArrayList<>();
        this.loanItems = new ArrayList<>();
        this.numOfReservedItems = 0;
        this.numOfLoanItems = 0;
    }

    // Getters **********************************************************************
    public ArrayList<T> getReservedItems() {
        return reservedItems;
    }

    public ArrayList<T> getLoanItems() {
        return loanItems;
    }

    public double getNumOfReservedItems() {
        return numOfReservedItems;
    }

    public double getNumOfLoanItems() {
        return numOfLoanItems;
    }

    // Setters **********************************************************************
    public void setNumOfReservedItems(double newNumOfReservedItems) {
        this.numOfReservedItems = newNumOfReservedItems;
    }

    public void setNumOfLoanItems(double newNumOfLoanItems) {
        this.numOfLoanItems = newNumOfLoanItems;
    }

    // Methods **********************************************************************

    /**
     * Method to try and create a reserve of an Item belonging to a library
     *
     * @param id Item id to be reserved.
     * @param library library that contains the Item to be reserved.
     * @param user user that is going to reserve the Item.
     * @return true on success and false on failure.
     */
    public Reserve reserveItem(String id, Library library, User user) {
        int index_item = 0;
        for(int i = 0; i < library.items.size(); i++) {
            if(id == library.items.get(i)) {
                index_item = i;
                break;
            }
            if(i == (library.items.size() - 1)) {
                System.out.println("Failure to reserve the item :" + id);
                return;
            }
        }
        Reserve reserve = new Reserve(library, library.items.get(index_item), user);
        return reserve;
    }

    /**
     * Method to try and create a loan of an Item belonging to a library
     *
     * @param id Item id to be loaned.
     * @param library library that contains the Item to be loaned.
     * @param staff staff that is going to loan the Item.
     * @return true on success and false on failure.
     */
    public Loan loanItem(String id, Item item, LibraryStaff staff) {
        int index_item = 0;
        for(int i = 0; i < library.items.size(); i++) {
            if(id == library.items.get(i)) {
                index_item = i;
                break;
            }
            if(i == (library.items.size() - 1)) {
                System.out.println("Failure to loan the item :" + id);
                return;
            }
        }
        Loan loan = new Loan(library, library.items.get(index_item), staff);

        return loan;        
    }

    /**
     * Method to return an Item belonging to a library that was loaned.
     *
     * @param loan loan of a Item.
     * @param retrievalDate retrieval date of a item.
     * @return true on success and false on failure.
     */
    public boolean returnItem(Loan loan, LocalDate retrievalDate) {
        if(loan == null){
            return false;
        }
        if(loan.retrieved == true) {
            return false;
        }
        loan.setRetrievalDate(retrievalDate);
        loan.setRetrieved(true);
        loan.isActive(false);
        return true;        
    }
}