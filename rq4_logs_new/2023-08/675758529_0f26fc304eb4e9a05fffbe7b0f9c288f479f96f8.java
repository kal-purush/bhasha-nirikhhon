package lesson7.Apple;

import java.lang.reflect.Field;

/**
 Создать класс Apple и добавить в него поле color с модификатором доступа private и
 инициализировать его. В методе main другого класса создать объект Apple, и не
 используя сеттеры изменить значение поля color.
 */
public class Main{
    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {
        Apple apple = new Apple();
        System.out.println(apple.color);

        apple.changeMyPrivateField("Red");

        System.out.println(apple.color);
    }
}