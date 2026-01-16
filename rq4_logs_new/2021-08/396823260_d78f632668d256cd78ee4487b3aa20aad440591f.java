package my_spring.object_factory.annotation.handlers;

import lombok.SneakyThrows;
import my_spring.object_factory.ObjectFactory;
import my_spring.object_factory.annotation.Inject;
import my_spring.object_factory.annotation.handlers.AnnotationHandler;

import java.lang.reflect.Field;

public class InjectAnnotationHandler implements AnnotationHandler {
    @SneakyThrows
    @Override
    public void handle(Object declaredObject) {
        for (Field field : declaredObject.getClass().getDeclaredFields()) {
            if (field.isAnnotationPresent(Inject.class)) {
                Object o = ObjectFactory.getInstance().createObject(field.getType());
                field.setAccessible(true);
                field.set(declaredObject, o);
            }
        }
    }
}