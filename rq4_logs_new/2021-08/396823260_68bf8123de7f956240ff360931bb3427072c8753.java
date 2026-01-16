package my_spring.object_factory.annotation.proxy;

import lombok.SneakyThrows;
import my_spring.object_factory.annotation.proxy.handlers.ProxyAnnotationCreatorHandler;
import org.reflections.Reflections;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

public class ProxyAnnotationsCreatorImpl implements ProxyAnnotationsCreator {
    private final Reflections scanner = new Reflections("my_spring.object_factory.annotation.proxy.handlers");
    private final List<ProxyAnnotationCreatorHandler> annotationHandlers = new ArrayList<>();

    @SneakyThrows
    public ProxyAnnotationsCreatorImpl() {
        for (var handler : scanner.getSubTypesOf(ProxyAnnotationCreatorHandler.class) ) {
            if (!Modifier.isAbstract(handler.getModifiers())) {
                annotationHandlers.add(handler.getDeclaredConstructor().newInstance());
            }
        }
    }

    @Override
    public <T> T createProxy(T obj) {
        for (ProxyAnnotationCreatorHandler annotationHandler : annotationHandlers) {
            obj = annotationHandler.create(obj);
        }

        return obj;
    }
}