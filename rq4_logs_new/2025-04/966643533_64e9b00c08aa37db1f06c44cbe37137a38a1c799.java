package org.spiceboys.Travel.Diary.model;

import com.fasterxml.jackson.annotation.JsonBackReference;
import jakarta.persistence.*;

@Entity
@Table(name = "favourites")
public class Favourite {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer favouriteId;

    @OneToOne
    private Itinerary itinerary;

    @ManyToOne
    @JoinColumn(
            name = "userId"
    )
    @JsonBackReference
    private User user;

    public Favourite(User user, Itinerary itinerary) { /
        this.user = user;
        this.itinerary = itinerary;
    }

    public Favourite() {}

    public Integer getFavouriteId() {
        return favouriteId;
    }

    public void setFavouriteId(Integer favouriteId) {
        this.favouriteId = favouriteId;
    }

    public Itinerary getItinerary() {
        return itinerary;
    }

    public void setItinerary(Itinerary itinerary) {
        this.itinerary = itinerary;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }
}