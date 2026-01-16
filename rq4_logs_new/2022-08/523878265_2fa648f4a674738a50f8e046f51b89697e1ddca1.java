package org.citybike.dto.constraint;

import org.citybike.dto.JourneyRequest;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class DateRangeValidator implements ConstraintValidator<ValidDateRange, JourneyRequest> {
    @Override
    public void initialize(ValidDateRange constraintAnnotation) {
        ConstraintValidator.super.initialize(constraintAnnotation);
    }

    @Override
    public boolean isValid(JourneyRequest journeyRequest, ConstraintValidatorContext context) {
        if (journeyRequest.getDepartureTimeStamp() == null) {
            return false;
        }
        if (journeyRequest.getReturnTimestamp() == null) {
            return false;
        }
        return journeyRequest.getDepartureTimeStamp().toInstant()
                .isBefore(journeyRequest.getReturnTimestamp().toInstant());
    }
}