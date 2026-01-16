package org.fenxui.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used to register application pages. You can have one or more pages, all of
 * which will be stacked on top of one another with only the first page visible
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface AppPage {

	String value();

	String cssClass() default "apppage";
}