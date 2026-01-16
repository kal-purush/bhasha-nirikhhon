/*
 * Реализуйте класс "Прямоугольник" (Rectangle), который имеет свойства ширина (width) и высота (height).
 * Класс должен обладать следующими методами:

Конструктор без параметров, который создает прямоугольник с шириной и высотой по умолчанию.
Конструктор, который принимает значения ширины и высоты и создает прямоугольник с заданными значениями.
Методы доступа (геттеры и сеттеры) для свойств ширины и высоты.
Метод "вычислить площадь" (calculateArea), который возвращает площадь прямоугольника (ширина * высота).
Метод "вычислить периметр" (calculatePerimeter), который возвращает периметр прямоугольника
(2 * (ширина + высота)).
 * 
 */
package lesson1.task2;

public class Rectangle {
    private int width;
    private int height;

    public Rectangle() {
        this.width = 15;
        this.height = 30;
    }

    public Rectangle(int width, int height) {
        this.width = width;
        this.height = height;
    }

    public int getWidth() {
        return width;
    }
    public void setWidth(int width) {
        this.width = width;
    }
    public int getHeight() {
        return height;
    }
    public void setHeight(int height) {
        this.height = height;
    } 

    public int calculateArea () {
        return width * height;
    }

    public int calculatePerimeter() {
        return 2*(width + height);
    }
    
}