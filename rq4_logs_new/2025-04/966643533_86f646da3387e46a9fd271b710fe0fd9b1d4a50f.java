package org.spiceboys.Travel.Diary.dto;

import org.spiceboys.Travel.Diary.model.Itinerary;

public class FavouriteDTO {
    private Long favouriteId;
    private Long userId;
    private String username;
    private Itinerary itinerary;

    public FavouriteDTO(Long favouriteId, Long userId, String username, Itinerary itinerary) {
        this.favouriteId = favouriteId;
        this.userId = userId;
        this.username = username;
        this.itinerary = itinerary;
    }

    public Long getFavouriteId() {
        return favouriteId;
    }

    public void setFavouriteId(Long favouriteId) {
        this.favouriteId = favouriteId;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Itinerary getItinerary() {
        return itinerary;
    }

    public void setItinerary(Itinerary itinerary) {
        this.itinerary = itinerary;
    }
}