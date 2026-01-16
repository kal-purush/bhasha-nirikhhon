package in.perpixl.movie.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.validation.Constraint;
import javax.validation.Payload;

import in.perpixl.movie.validators.SwarValidator;

@Documented
@Constraint(validatedBy = SwarValidator.class)
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface SwarConstraint {
	String message() default "invalid swar found in data";
	Class<?>[] groups() default {};
	Class<? extends Payload>[] payload() default {};
}