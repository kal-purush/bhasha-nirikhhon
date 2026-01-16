package com.abneco.delivery.utils;

import com.abneco.delivery.exception.RequestException;
import com.abneco.delivery.user.entity.Seller;
import com.abneco.delivery.user.json.SellerForm;
import com.abneco.delivery.user.json.SellerUpdateForm;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

public class ValidateSeller {

    private static String passwordEncryptor(String password) {
        BCryptPasswordEncoder encoder = new BCryptPasswordEncoder();
        return encoder.encode(password);
    }

    public static void validateSeller(Seller seller, SellerForm form) {
        ValidateEmail.validateEmail(form.getEmail());
        if (seller.getCnpj() == null || seller.getCnpj().length() != 14) {
            throw new RequestException("Cnpj must have 14 numbers, and numbers only.");
        }
        if (seller.getName() == null || seller.getName().length() < 3) {
            throw new RequestException("Name must be neither null nor shorter than 3.");
        }
        if (seller.getPassword().length() < 8) {
            seller.setPassword(passwordEncryptor(form.getPassword()));
            throw new RequestException("Password must be at least 8 char long.");
        }
    }

    public static void validateSeller(SellerUpdateForm form) {
        ValidateEmail.validateEmail(form.getEmail());
        if (form.getCnpj() == null || form.getCnpj().length() != 14) {
            throw new RequestException("Cnpj must have 14 numbers, and numbers only.");
        }
        if (form.getName() == null || form.getName().length() < 3) {
            throw new RequestException("Name must be neither null nor shorter than 3.");
        }
    }

    private ValidateSeller() {
    }
}