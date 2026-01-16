package my_spring.object_factory.annotation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class AnnotationsProcessorImpl implements AnnotationsProcessor {
    private final Map<Class<? extends Annotation>, AnnotationHandler> annotation2AnnotationHandler = new HashMap<>();

    public AnnotationsProcessorImpl() {
        annotation2AnnotationHandler.put(RandomInteger.class, new RandomIntegerAnnotationHandler());
        annotation2AnnotationHandler.put(Inject.class, new InjectAnnotationHandler());
    }

    @Override
    public void processField(Object declaringObject, Field field) {
        for (Map.Entry<Class<? extends Annotation>, AnnotationHandler> classAnnotationHandlerEntry : annotation2AnnotationHandler.entrySet()) {
            if (field.isAnnotationPresent(classAnnotationHandlerEntry.getKey())) {
                classAnnotationHandlerEntry.getValue().handle(declaringObject, field);
            }
        }
    }

    @Override
    public void processMethod(Object declaringObject, Method method) {

    }
}