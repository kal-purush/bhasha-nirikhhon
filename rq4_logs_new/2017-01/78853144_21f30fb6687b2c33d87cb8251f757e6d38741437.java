package com.tw.tradeaway.domain;

import org.hibernate.annotations.Type;

import javax.persistence.*;
import java.time.LocalDate;

/**
 * Created by poojadhupar on 1/18/17.
 */
@Entity
@Table(name="sellers")
public class Seller {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name="name", length = 40, nullable = false)
    private String name;

    @Column(name="email", unique=true, length = 40, nullable = false)
    private String email;

    @Column(name="address", length = 100, nullable = false)
    private String address;

    @Column(name="mobile", length = 10, nullable = false)
    private String mobile;

    @Column(name="pan_number", nullable = false)
    private String pan_number ;

    @Column(name="experience_in_months", nullable = false)
    private Integer experience_in_months;

    @JoinColumn(name="user_id", nullable = false)
    @OneToOne
    private User user;

    public Seller(){

    }
    public Seller(User user , String name, String email, String address, String mobile ,String pan_number ,Integer experience_in_months) {

        this.pan_number = pan_number;
        this.experience_in_months = experience_in_months;
        this.user = user;
        this.name = name;
        this.email = email;
        this.address = address;
        this.mobile = mobile;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Integer getExperienceInMonths() {
        return experience_in_months;
    }

    public void setExperienceInMonths(Integer experience_in_months) {
        this.experience_in_months = experience_in_months;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public String getPanNumber() {
        return pan_number;
    }

    public void setPanNumber(String pan_number) {
        this.pan_number = pan_number;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }


    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }

}