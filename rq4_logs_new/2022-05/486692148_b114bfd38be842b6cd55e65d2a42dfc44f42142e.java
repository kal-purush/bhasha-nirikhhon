package com.example.storeeverything.validation;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;

@Target( { FIELD, METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Constraint(validatedBy = LowerCaseValidator.class)
public @interface LowerCaseValidation {
    public String message() default "{lowerCase}";

    public Class<?>[] groups() default {};

    public Class<? extends Payload>[] payload() default {};
}