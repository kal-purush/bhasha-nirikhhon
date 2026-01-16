package org.milan.core.interfaces;

/**
 * Implementation of {@link Java8Interface}
 *
 * @author Milan Rathod
 */
public class Java8InterfaceImpl implements Java8Interface {

    @Override
    public void testMethod() {
        System.out.println("Test Method");
    }

    public static void main(String[] args) {
        new Java8InterfaceImpl().testMethod();
        System.out.println(Java8Interface.testStaticMethod());
        System.out.println(new Java8InterfaceImpl().testDefaultMethod());
    }
}