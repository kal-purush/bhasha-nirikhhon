package com.github.fernthedev.core.api;

import java.lang.annotation.*;

/**
 * Used to show that the method is ASYNC
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.METHOD})
@APIUsage
@Documented
public @interface Async {}