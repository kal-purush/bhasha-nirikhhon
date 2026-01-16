package com.abneco.delivery.user.json.buyer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class BuyerUpdateForm extends BuyerForm {

    private String id;

    public BuyerUpdateForm(String id, String newName, String email, Long phoneNumber, String cpf) {
        setId(id);
        setName(newName);
        setEmail(email);
        setPhoneNumber(phoneNumber);
        setCpf(cpf);
    }
}