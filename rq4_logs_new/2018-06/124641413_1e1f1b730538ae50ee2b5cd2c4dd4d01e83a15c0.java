package org.launchcode.projectmanager;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = UniqueProjectTitleValidator.class)
public @interface UniqueProjectTitle {

    String message() default "You already have a composition with that exact title";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};
}