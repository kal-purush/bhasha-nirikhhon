package com.livenow.week2.domain;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Embeddable;

@Data
@Embeddable
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Address {

    private String zip;
    private String code;
    private String number;

    public Address(String zip, String code, String number) {
        this.zip = zip;
        this.code = code;
        this.number = number;
    }
}