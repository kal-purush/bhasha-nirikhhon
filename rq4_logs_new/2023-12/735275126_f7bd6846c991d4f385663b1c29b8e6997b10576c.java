package com.kfh.education.validation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = ArabicCharactersValidator.class)
@Documented
public @interface ArabicCharacters {
    String message() default "Field should contain only Arabic characters";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}