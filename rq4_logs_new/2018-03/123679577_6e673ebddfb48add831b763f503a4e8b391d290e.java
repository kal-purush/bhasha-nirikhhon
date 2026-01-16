package users.app.dummyx.qenawi.injection;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import javax.inject.Qualifier;

/**
 * Created by ahmedkamal on 11.3.2018.
 */

@Qualifier
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface RunOn
{
    SchedulerType value() default SchedulerType.IO;
}