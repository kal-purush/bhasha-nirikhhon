package com.abneco.delivery.address.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class AddressUpdateForm {

    @NotNull
    private String addressId;

    @NotNull
    private String userId;
    @NotNull
    @Size(min = 8, max = 8)
    private String cep;
    private String complemento;

    @NotNull
    private Integer numero;


}