package com.backupreality.shared.exceptions;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Use this class to annotate methods in {@link ExceptionTranslationSource} which should
 * be used for translations.
 * The signature of this method should accept and return an instance of a class inherited
 * from {@link Exception}.
 * <b>Note:</b>
 * Since generic arguments can not be determined at runtime the return type should be plain
 * object (not {@link java.util.Optional}. However you can safely return {@code null} from
 * this method in order to skip translation.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ExceptionTranslation
{
}
