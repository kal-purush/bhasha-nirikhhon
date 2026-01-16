package org.spiceboys.Travel.Diary.model;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Entity
@Table(name="itineraries")
public class Itinerary {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long itineraryId;

    @ManyToOne
    @JoinColumn(name="user_id")
    private User  user;

    @ManyToOne
    @JoinColumn(name="country_id")
    private Country country;

    @NotNull
    private Boolean isPrivate;

    @NotBlank
    private Date modifiedAt;

    @NotBlank
    private String itineraryTitle;

    public Itinerary() {
    }

    public Itinerary(Long itineraryId, String itineraryTitle, User user, Country country, Boolean isPrivate, Date modifiedAt) {
        this.itineraryId = itineraryId;
        this.itineraryTitle = itineraryTitle;
        this.user = user;
        this.country = country;
        this.isPrivate = isPrivate;
        this.modifiedAt = modifiedAt;
    }

    public Long getItineraryId() {
        return itineraryId;
    }

    public void setItineraryId(Long itineraryId) {
        this.itineraryId = itineraryId;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public Country getCountry() {
        return country;
    }

    public void setCountry(Country country) {
        this.country = country;
    }

    public Boolean getPrivate() {
        return isPrivate;
    }

    public void setPrivate(Boolean aPrivate) {
        isPrivate = aPrivate;
    }

    public Date getModifiedAt() {
        return modifiedAt;
    }

    public void setModifiedAt(Date modifiedAt) {
        this.modifiedAt = modifiedAt;
    }

    public String getItineraryTitle() {
        return itineraryTitle;
    }

    public void setItineraryTitle(String itineraryTitle) {
        this.itineraryTitle = itineraryTitle;
    }
}
