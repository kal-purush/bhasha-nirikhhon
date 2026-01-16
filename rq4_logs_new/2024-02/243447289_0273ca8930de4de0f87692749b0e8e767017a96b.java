package de.aservo.confapi.commons.http;

import javax.ws.rs.HttpMethod;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target(value=METHOD)
@Retention(value=RUNTIME)
@HttpMethod(value="PATCH")
@Documented
public @interface PATCH {
}