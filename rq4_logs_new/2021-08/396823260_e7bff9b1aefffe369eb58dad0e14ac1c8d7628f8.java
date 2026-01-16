package real_spring;

import com.github.javafaker.Faker;
import lombok.SneakyThrows;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

import java.lang.reflect.Field;

public class InjectRandomNameAnnotationBeanPostProcessor implements BeanPostProcessor {
    private final Faker faker = new Faker();

    @Override
    @SneakyThrows
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        Class<?> type = bean.getClass();
        for (Field field : type.getDeclaredFields()) {
            InjectRandomName annotation = field.getAnnotation(InjectRandomName.class);
            if (annotation != null) {
                field.setAccessible(true);
                field.set(bean, faker.gameOfThrones().character());
            }
        }
        return bean;
    }

}