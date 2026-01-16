package models.library.management;

import models.library.content.Item;
import models.people.Person;
import java.time.LocalDate;

public class ItemReservation extends Reserve {
    // Private Attributes ---------------------------------------------------------------
    private final Item item;
    private final LocalDate pickupDate;

    // Constructor ----------------------------------------------------------------------
    public ItemReservation(Person user, Item item) {
        super(user);
        this.item = item;
        this.pickupDate = LocalDate.now();    // Pickup date logic yet to be implemented.
    }

    // Getters --------------------------------------------------------------------------
    public Item getItem() {
        return item;
    }

    public LocalDate getPickupDate() {
        return pickupDate;
    }
}