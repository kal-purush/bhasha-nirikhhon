package com.example.cargo_transportation.annotations;

import com.example.cargo_transportation.validations.UniqueValidator;
import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Documented
@Constraint(validatedBy = {UniqueValidator.class})
@Target({TYPE})
@Retention(RUNTIME)
public @interface Unique{

    String message() default "Klinton90.unique";

    UniqueColumn[] columns() default {};

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

}