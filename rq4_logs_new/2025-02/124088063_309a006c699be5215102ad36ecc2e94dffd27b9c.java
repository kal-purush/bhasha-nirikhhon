package testcase;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Mark classes or tests with this annotation af you want to be skipped from retrying.
 */
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface NonRetryable {
}