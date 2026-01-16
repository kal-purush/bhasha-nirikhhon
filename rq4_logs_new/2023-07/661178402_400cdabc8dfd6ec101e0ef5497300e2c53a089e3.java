package wsffs.springframework.beans.factory;

import wsffs.springframework.beans.BeansException;

public interface BeanFactory {

    /**
     * Return an instance, which may be shared or independent, of the specified bean.
     * <p>Behaves the same as {@link #getBean(String)}, but provides a measure of type
     * safety by throwing a BeanNotOfRequiredTypeException if the bean is not of the
     * required type. This means that ClassCastException can't be thrown on casting
     * the result correctly, as can happen with {@link #getBean(String)}.
     * <p>Translates aliases back to the corresponding canonical bean name.
     * <p>Will ask the parent factory if the bean cannot be found in this factory instance.
     *
     * shared이거나 독립적인 지정된 빈의 인스턴스를 반환합니다.
     * 동작은 getBean(String)과 동일하나 빈이 요구한 타입과 다른 경우 BeanNotOfRequiredTypeException를
     * 발생시키는 것으로 타입 안정성을 제공합니다. 이는 캐스팅 결과가 올바르다면 getBean(String)에서 발생하는
     * ClassCastException이 여기서는 발생하지 않음을 의미합니다.
     * alias를 적절한 정식(canonical) 빈 이름으로 변환합니다.
     * 이 팩토리 인스턴스에서 빈을 찾을 수 없는 경우에 부모 팩토리에 질의하게 됩니다.
     * 역자 주: beanFactory는 hierarchy를 가질 수 있습니다.
     * 역자 코멘트: 학습 용도로 번역해둔 주석입니다. 참고만 하세요.
     */
    <T> T getBean(String name, Class<T> requiredType) throws BeansException;
}