package ru.nsu.peretyatko.validator.equipments;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;
import ru.nsu.peretyatko.dto.equipments.EquipmentCategoryRequest;
import ru.nsu.peretyatko.validator.DefaultValidator;

@Component
@RequiredArgsConstructor
public class EquipmentCategoryValidator extends DefaultValidator implements Validator {
    @Override
    public boolean supports(Class<?> clazz) {
        return clazz.equals(EquipmentCategoryRequest.class);
    }

    @Override
    public void validate(Object target, Errors errors) {
        createAndThrowException(errors);
    }
}