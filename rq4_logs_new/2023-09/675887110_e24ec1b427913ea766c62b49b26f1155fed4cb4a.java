package library.management;

import java.time.LocalDate;

import library.Library;
import library.Multimedia;
import people.users.*;
import people.staff.*;

/**
 * Class that represents a reserve of an item.
 *
 * @author Matheus Reato (RA: 244088), Caio Taishi (RA: 242908).
 */

public class Reserve {
    // Private Attributes ---------------------------------------------------------------
    private Library library;
    private Multimedia item;
    private User user;
    private LibraryStaff libStaff;
    private LocalDate reserveDate;
    private LocalDate pickupDate;

    // Constructor (reserved by user) ---------------------------------------------------
    public Reserve(Library library, Multimedia item, User user) {
        if (!this.user.getIsSuspended()) {
            this.library = library;
            this.item = item;
            this.user = user;
            this.libStaff = null;

            // Determining the pickup date.


        } else {
            this.library = null;
            this.item = null;
            this.user = null;
            this.libStaff = null;

            System.out.println("Sorry, this item can't be reserved.");
        }

        this.reserveDate = LocalDate.now();
    }

    // Constructor (reserved by library staff)
    public Reserve(Library library, Multimedia item, LibraryStaff libStaff) {
        if (!this.libStaff.getIsSuspended()) {
            this.library = library;
            this.item = item;
            this.user = null;
            this.libStaff = libStaff;
        }
        else {
            this.library = null;
            this.item = null;
            this.user = null;
            this.libStaff = null;

            System.out.println("Sorry, this item can't be reserved.");
        }
    }

    // Getters --------------------------------------------------------------------------
    public Library getLibrary() {
        return library;
    }

    public Multimedia getItem() {
        return item;
    }

    public User getUser() {
        return user;
    }

    public LibraryStaff getLibStaff() {
        return libStaff;
    }

    public LocalDate getReserveDate() {
        return reserveDate;
    }

    public LocalDate getPickupDate() {
        return pickupDate;
    }
}