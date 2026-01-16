package com.schoolmanagement.schoolmanagement.customAnnotations.classes;

import com.schoolmanagement.schoolmanagement.customAnnotations.interfaces.ValidatedOtp;
import com.schoolmanagement.schoolmanagement.entity.UserOtp;
import com.schoolmanagement.schoolmanagement.service.OtpService;
import org.springframework.beans.factory.annotation.Autowired;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.Optional;

public class ValidatedOtpValidator implements ConstraintValidator<ValidatedOtp, String> {

    @Autowired
    OtpService otpService;

    @Override
    public boolean isValid(String token, ConstraintValidatorContext constraintValidatorContext) {
        Optional<UserOtp> optionalUserOtp = Optional.ofNullable(otpService.findByToken(token));
        if (!optionalUserOtp.isPresent())
            return false;
        if (validatedOtp(optionalUserOtp)) {
            return true;
        }
        return false;
    }

    private boolean validatedOtp(Optional<UserOtp> optionalUserOtp) {
        return optionalUserOtp.get().getOtpValidated() != null && optionalUserOtp.get().getOtpValidated().equals("Y");
    }
}