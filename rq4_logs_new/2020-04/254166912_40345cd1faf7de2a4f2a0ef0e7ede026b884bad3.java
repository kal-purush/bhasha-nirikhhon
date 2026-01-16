package com.example.eventz;

import java.util.ArrayList;
import java.util.List;

public class Profile {

    private String email;
    private String imageURL;
    private List<Event> tickets;
    private List<Event> favourites;

    public Profile(String email) {
        this.email = email;
        imageURL = null;
        this.tickets = new ArrayList<>();
        this.favourites = new ArrayList<>();
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getImageURL() {
        return imageURL;
    }

    public void setImageURL(String imageURL) {
        this.imageURL = imageURL;
    }

    public List<Event> getTickets() {
        return tickets;
    }

    public void setTickets(List<Event> tickets) {
        this.tickets = tickets;
    }

    public List<Event> getFavourites() {
        return favourites;
    }

    public void setFavourites(List<Event> favourites) {
        this.favourites = favourites;
    }
}