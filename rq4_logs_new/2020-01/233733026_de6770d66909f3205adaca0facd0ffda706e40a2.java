package com.company;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;

public class Menu {
    private LocalDate lastUpdated;
    private ArrayList<MenuItem> menuArray;

    //getters
    public LocalDate getLastUpdated() {
        return lastUpdated;
    }
    public ArrayList<MenuItem> getMenuArray() {
        return menuArray;
    }

    //setters
    // I haven't tested it, but this method should set lastUpdated to the current date.
    public void updateLastUpdated() {
        this.lastUpdated = java.time.LocalDate.now();
    }
    public void addMenuItem(MenuItem newMenuItem) {
        this.menuArray.add(newMenuItem);
    }
    public void menuItemNoLongerNew(MenuItem existingMenuItem) {
        existingMenuItem.setIsNew(false);
    }
}