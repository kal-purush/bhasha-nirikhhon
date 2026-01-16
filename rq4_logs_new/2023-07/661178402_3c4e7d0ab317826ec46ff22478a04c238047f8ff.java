package wsffs.springframework.beans.factory.config;

import java.util.ArrayList;
import java.util.List;

public class DefaultBeanDefinition implements BeanDefinition {

    public DefaultBeanDefinition() {
    }

    private Class<?> beanClass;
    private final List<String> beanNamesForDependencies = new ArrayList<>();

    @Override
    public void setBeanClass(Class<?> beanClass) {
        this.beanClass = beanClass;
    }

    @Override
    public Class<?> getBeanClass() {
        return beanClass;
    }

    @Override
    public void setDependsOn(String... dependsOn) {
        beanNamesForDependencies.addAll(List.of(dependsOn));
    }

    @Override
    public String[] getDependsOn() {
        return beanNamesForDependencies.toArray(new String[0]);
    }
}