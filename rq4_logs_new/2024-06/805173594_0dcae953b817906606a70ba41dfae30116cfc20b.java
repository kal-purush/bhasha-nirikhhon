package com.example.BookingTravelBackend.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Entity
@Getter
@Setter
@Table (name = "HotelPartners")
public class HotelPartners {
    @Id
    @GeneratedValue (strategy = GenerationType.IDENTITY)
    @Column (name = "PartnersId")
    private int id;

    @Column (name = "HotelName", columnDefinition = "nvarchar(255)")
    private String hotelName;

    @Column (name = "Phone")
    private String phone;

    @Column (name = "Email")
    private String email;

    @OneToMany (mappedBy = "Partner", cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    private List<Hotel> listHotel;

    @OneToMany (mappedBy = "hotelPartners", cascade = CascadeType.ALL,fetch = FetchType.EAGER)
    private List<PartnersManager> listManager;

    @OneToMany (mappedBy = "partnerReview", cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    private List<ReviewPartner> listReviews;

}