package com.abneco.delivery.address.json;

import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

@Getter
@Setter
public class BaseAddressForm {

    @NotNull
    private String userId;
    @NotNull
    @Size(min = 8, max = 8)
    private String cep;
    private String complemento;

    @NotNull
    private Integer numero;

    public BaseAddressForm(String userId, String cep, String complemento, Integer numero) {
        this.userId = userId;
        this.cep = cep;
        this.complemento = complemento;
        this.numero = numero;
    }

    public BaseAddressForm() {
    }
}