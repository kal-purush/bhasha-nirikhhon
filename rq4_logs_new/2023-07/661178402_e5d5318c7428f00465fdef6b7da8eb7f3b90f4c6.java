package wsffs.springframework.beans.factory.config;

public interface BeanDefinition {

    void setBeanClass(Class<?> beanClass);

    Class<?> getBeanClass();

    /**
     * Set the names of the beans that this bean depends on being initialized.
     * The bean factory will guarantee that these beans get initialized first.
     *
     * 이 bean이 초기화 되는데 의존하는 빈들의 이름을 설정합니다.
     * bean factory는 이 빈들이 우선적으로 초기화되어 있는 것을 보장합니다.
     */
    void setDependsOn(String... dependsOn);

    /**
     * Return the bean names that this bean depends on.
     * 이 빈이 의존하는 빈의 이름을 반환합니다.
     */
    String[] getDependsOn();
}