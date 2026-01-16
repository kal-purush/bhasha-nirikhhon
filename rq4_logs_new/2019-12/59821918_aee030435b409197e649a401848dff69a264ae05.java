package org.cplier.codegen.annotation;

import org.cplier.codegen.annotation.constraint.PackagePatternValidator;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.*;

@Documented
@Constraint(validatedBy = PackagePatternValidator.class)
@Target({ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface PackagePatternValidate {

  String message() default "Invalid package name";

  Class<?>[] groups() default {};

  Class<? extends Payload>[] payload() default {};
}