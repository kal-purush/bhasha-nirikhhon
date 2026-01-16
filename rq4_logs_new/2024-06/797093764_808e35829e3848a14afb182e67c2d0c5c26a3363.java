package mg.itu.prom16.annotation;

import java.lang.annotation.*;

@Target({ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME) 
public @interface ObjectParameter {
    String value() default "";
}