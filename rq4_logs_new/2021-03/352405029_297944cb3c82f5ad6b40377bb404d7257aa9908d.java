package io.fdlessard.codebites.customer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;


@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Address implements Serializable {

    private Long id;
    private String number;
    private String street;
    private String city;
    private String postalCode;
    private String province;
    private String country;

}