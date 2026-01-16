package com.bin448.backend.converter;

import com.bin448.backend.entity.Address;
import com.bin448.backend.entity.DTOentity.AddressDTO;

public class AddressConverter extends AbstractConverter {

    public static AddressDTO fromEntity(Address e) {
        AddressDTO addressDTO = new AddressDTO();
        addressDTO.setId(e.getId());
        addressDTO.setCityAddress(e.getCityAddress());
        addressDTO.setCity(e.getCity());
        addressDTO.setCountry(e.getCountry());
        return addressDTO;
    }

    public static Address toEntity(AddressDTO e) {
        Address address = new Address();
        address.setId(e.getId());
        address.setCityAddress(e.getCityAddress());
        address.setCity(e.getCity());
        address.setCountry(e.getCountry());
        return address;
    }
}