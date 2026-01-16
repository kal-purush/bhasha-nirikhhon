package com.ycwu.sqltemplate.annotation;

import com.ycwu.sqltemplate.autoconfigure.SqlTemplateAutoConfiguration;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Import;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Import({SqlTemplateAutoConfiguration.class})
@Documented
public @interface EnableSqlTemplate {

}