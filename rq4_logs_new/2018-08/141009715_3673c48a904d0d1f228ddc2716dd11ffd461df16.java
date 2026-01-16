//класс родитель
//методы класса обьявлены в интерфейсе

public abstract class Animals implements Interface {
    String name;
    String race;
    String color;
    int age;
    String type;

    @Override
    public Integer age_animal() {
        return age;
    }
    @Override
    public String toString() {
        //учитываем в потоке вывода все ли параметры заданы у объекта
        if(name == null)
            return "Животное: " + type + ". Цвет: " + color;
        else
            return "Животное: " + type + ". Имя: " + name + ". Род: " + race + ". Цвет: " + color + ". Возраст: " + age + " года/лет";
    }
}