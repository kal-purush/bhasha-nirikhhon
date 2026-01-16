package com.github.eduardovalentim.easymath.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Place holder annotation to be used in interfaces
 * 
 * @author eduardovalentim
 */
@Inherited
@Documented
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.CLASS)
public @interface EasyMath {

	public GenerationMode generationMode() default GenerationMode.JAVA;
}