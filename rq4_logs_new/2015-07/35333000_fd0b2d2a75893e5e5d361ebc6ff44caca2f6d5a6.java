package pl.btcgrouppl.btc.backend.commons.test.util.ddd;

import pl.btcgrouppl.btc.backend.commons.ddd.models.annotations.EventListenerAnnotation;

import java.lang.reflect.Method;

/**
 * Created by Sebastian Mekal <sebitg@gmail.com> on 01.07.15.
 * <p>
 *     DDD tests utility class
 * </p>
 */
public class DddUtil {

    /**
     * Getting consumer method
     * @return Method
     */
    public static Method getConsumerMethod(Object source) {
        Class<?> aClass = source.getClass();
        Method[] methods = aClass.getMethods();
        for(Method item: methods) {
            if(item.isAnnotationPresent(EventListenerAnnotation.class)) {
                return item;
            }
        }
        return null;
    }
}