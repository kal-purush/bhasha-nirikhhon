package dev.kyuelin.hibernate.model;

import javax.persistence.*;
import javax.swing.*;

/**
 * Created by kennethlin on 5/16/15.
 */
@Entity
@Table(name="ACTOR")
public class Actor {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name="ACTOR_ID")
    private int id = 0;
    private String firstname = null;
    private String lastname = null;
    private String shortname = null;
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "MOVIE_ID", nullable = false)
    private Movie movie=null;

    public Actor() {}

    public Actor (String firstname, String lastname, String shortname) {
        this.firstname=firstname;
        this.lastname=lastname;
        this.shortname=shortname;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getFirstname() {
        return firstname;
    }

    public void setFirstname(String firstname) {
        this.firstname = firstname;
    }

    public String getLastname() {
        return lastname;
    }

    public void setLastname(String lastname) {
        this.lastname = lastname;
    }

    public String getShortname() {
        return shortname;
    }

    public void setShortname(String shortname) {
        this.shortname = shortname;
    }

    public Movie getMovie() {
        return movie;
    }

    public void setMovie(Movie movie) {
        this.movie = movie;
    }
}