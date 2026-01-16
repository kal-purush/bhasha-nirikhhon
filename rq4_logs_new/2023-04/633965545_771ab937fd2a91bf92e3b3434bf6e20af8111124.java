package com.bdg.persistent;

import javax.persistence.*;
import java.sql.Timestamp;

@Entity
@Table(
        name = "pass_in_trip",
        uniqueConstraints = {@UniqueConstraint(columnNames = {"trip_number", "passenger_id", "place"})}
)
public class PassInTrip {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @ManyToOne
    @JoinColumn(name = "trip_number", referencedColumnName = "trip_number")
    private Trip trip;

    @ManyToOne
    @JoinColumn(name = "passenger_id", referencedColumnName = "id")
    private Passenger passenger;

    @Column(updatable = false, nullable = false)
    private Timestamp time;

    @Column(name = "place", nullable = false, updatable = false, length = 3)
    private String place;


    public PassInTrip() {
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Trip getTrip() {
        return trip;
    }

    public void setTrip(Trip trip) {
        this.trip = trip;
    }

    public Passenger getPassenger() {
        return passenger;
    }

    public void setPassenger(Passenger passenger) {
        this.passenger = passenger;
    }

    public Timestamp getTime() {
        return time;
    }

    public void setTime(Timestamp time) {
        this.time = time;
    }

    public String getPlace() {
        return place;
    }

    public void setPlace(String place) {
        this.place = place;
    }
}