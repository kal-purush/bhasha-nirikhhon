package com.training.courier.model;

import lombok.Data;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import java.time.LocalDateTime;

/**
 * Address entity
 */
@Entity
@Table(name = "address")
@Data
public class Address {

    /**
     * Unique identifier
     */
    @Column(name = "id")
    private Long id;

    /**
     * Country region
     */
    @Column(name = "region")
    private String region;

    /**
     * City
     */
    @Column(name = "city")
    private String city;

    /**
     * Street
     */
    @Column(name = "street")
    private String street;

    /**
     * House number
     */
    @Column(name = "house")
    private String house;

    /**
     * Apartment number
     */
    @Column(name = "apartment")
    private String apartment;

    /**
     * Registration time
     */
    @CreationTimestamp
    @Column(name = "created")
    private LocalDateTime created;

    /**
     * Update time
     */
    @UpdateTimestamp
    @Column(name = "updated")
    private LocalDateTime updated;
}