package org.citybike.dto.constraint;


import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.METHOD, ElementType.FIELD})
@Constraint(validatedBy = UniqueIdentifierValidator.class)
@Retention(RetentionPolicy.RUNTIME)
public @interface UniqueIdentifier {
    String message();
    Class<?>[] groups() default { };
    Class<? extends Payload>[] payload() default { };
}
