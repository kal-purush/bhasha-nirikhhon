package com.employeemanage.valid;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.validation.Constraint;
import javax.validation.Payload;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Constraint(validatedBy = {ByteValidator.class})
public @interface Byte {

	int min() default 0;
	int max() default Integer.MAX_VALUE;
	String charset() default "UTF-8";

	Class<?>[] groups() default {};
	String message() default "{charset}で{min}バイトから{max}バイトにしてください";
	Class<? extends Payload>[] payload() default{};

	@Target({ElementType.FIELD})
	@Retention(RetentionPolicy.RUNTIME)
	@Documented
	@interface List {
		Byte[] value();
	}
}