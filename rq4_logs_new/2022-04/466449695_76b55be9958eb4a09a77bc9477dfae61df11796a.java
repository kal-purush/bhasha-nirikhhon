package com.mikaelsonbraz.parkingapi.api.address.model.entity;


import com.mikaelsonbraz.parkingapi.api.city.model.entity.City;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table
public class Address {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer idAddress;

    @Column
    private String cep;

    @Column
    private String addressDetails;

    @Column
    private String district;

    @ManyToOne
    @JoinColumn(name = "id_city")
    private City city;
}