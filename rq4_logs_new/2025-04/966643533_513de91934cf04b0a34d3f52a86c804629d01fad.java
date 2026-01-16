package org.spiceboys.Travel.Diary.model;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name="countries")
public class Country {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer countryId;

    @Column(unique = true)
    private String countryName;

    @NotBlank
    private String description;

    @NotBlank
    private String countryPicUrl;

    public Country() {

    }

    public Country(String countryName, Integer countryId, String description, String countryPicUrl) {
        this.countryName = countryName;
        this.countryId = countryId;
        this.description = description;
        this.countryPicUrl = countryPicUrl;
    }

    public Integer getCountryId() {
        return countryId;
    }

    public void setCountryId(Integer countryId) {
        this.countryId = countryId;
    }

    public String getCountryName() {
        return countryName;
    }

    public void setCountryName(String countryName) {
        this.countryName = countryName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getCountryPicUrl() {
        return countryPicUrl;
    }

    public void setCountryPicUrl(String countryPicUrl) {
        this.countryPicUrl = countryPicUrl;
    }
}