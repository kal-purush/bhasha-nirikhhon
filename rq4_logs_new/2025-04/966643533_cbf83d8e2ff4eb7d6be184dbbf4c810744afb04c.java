package org.spiceboys.Travel.Diary.model;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;

@Entity
@Table(name="activity")
public class Activity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;


    @ManyToOne
    @JoinColumn(
            name = "itineraryId"
    )
    @JsonBackReference
    public  Itinerary itinerary;

    @NotBlank
    private String title;

    private Boolean completeStatus;

    private List<Note> notes;

    private List<Photo> photos;

    public Activity(String title, Boolean completeStatus) {
        this.title = title;
        this.completeStatus = completeStatus;
        this.notes = notes;
        this.photos = photos; //is this correct?
    }

    public Activity() {}

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Boolean getCompleteStatus() {
        return completeStatus;
    }

    public void setCompleteStatus(Boolean completeStatus) {
        this.completeStatus = completeStatus;
    }

    public List<Note> getNotes() {
        return notes;
    }

    public void setNotes(List<Note> notes) {
        this.notes = notes;
    }

    public List<Photo> getPhotos() {
        return photos;
    }

    public void setPhotos(List<Photo> photos) {
        this.photos = photos;
    }
}