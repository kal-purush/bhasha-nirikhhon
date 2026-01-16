package ru.yandex.practicum.filmorate.validator;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;

@Target({FIELD, PARAMETER} )
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = { DateReleaseValidator.class })
public @interface DateRelease {

    String message() default "Дата релиза не должна быть раньше 28.12.1895";

    Class<?>[] groups() default { };

    Class<? extends Payload>[] payload() default { };
}