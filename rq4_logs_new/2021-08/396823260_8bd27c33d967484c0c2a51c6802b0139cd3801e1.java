package my_spring.object_factory;

import lombok.SneakyThrows;

public class ObjectFactory {
    private final static ObjectFactory instance = new ObjectFactory();

    public static ObjectFactory getInstance() {
        return instance;
    }

    private final ObjectFactoryConfiguration objectFactoryConfiguration = new ObjectFactoryConfiguration();
    @SneakyThrows
    public <T> T createObject(Class<T> type) {
        if (type.isInterface()) {
            type = objectFactoryConfiguration.getInstance(type);
        }

        return type.getDeclaredConstructor().newInstance();
    }
}