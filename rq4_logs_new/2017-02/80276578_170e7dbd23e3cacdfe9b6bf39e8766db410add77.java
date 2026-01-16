package org.zkoss.reference.developer.spring.security.model;

import org.hibernate.validator.constraints.NotEmpty;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.util.Date;

@Entity
@Table(name = "movement_history")
public class MovementHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @NotNull
    @Temporal(TemporalType.DATE)
    @Column(name = "DATE")
    private Date date;

    @NotEmpty
    @ManyToOne
    @JoinColumn(name = "REQUEST_ID", nullable = false)
    private Request order;

    @NotEmpty
    @ManyToOne
    @JoinColumn(name = "PLACE_ID")
    private Place place;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }


    public Request getOrder() {
        return order;
    }

    public void setOrder(Request order) {
        this.order = order;
    }

    public Place getPlace() {
        return place;
    }

    public void setPlace(Place place) {
        this.place = place;
    }
}