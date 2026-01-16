package lesson8.shapes;

import java.text.DecimalFormat;

/**
 * Задача 2:
 * Написать иерархию классов Фигура, Треугольник, Прямоугольник, Круг. Реализовать
 * функцию подсчета площади для каждого типа фигуры и подсчет периметра(используя
 * абстрактный класс/методы). Создать массив из 5 разных фигур. Вывести на экран сумму
 * периметра всех фигур в массиве.
 */
public class Main {
    public static void main(String[] args) {
        Shapes triangle = new Triangle(10, 10, 3);
        DecimalFormat df = new DecimalFormat("#.###");
        System.out.println("Периметр треугольника со сторонами " + ((Triangle) triangle).getA() + " " + ((Triangle) triangle).getB() + " " + ((Triangle) triangle).getC() + " равен " + triangle.perimetr());
        System.out.println("А площадь " + df.format(triangle.area()));

        Shapes circle = new Circle(3);
        System.out.println("Периметр круга с радиусом " + ((Circle) circle).getR() + " Равен " + circle.perimetr());
        System.out.println("A площадь " + df.format(circle.area()));

        Shapes rectangle = new Rectangle(2, 3);
        System.out.println("Периметр прямоугольника со сторонами " + ((Rectangle) rectangle).getA() + " " + ((Rectangle) rectangle).getB() + " равен " + rectangle.perimetr());
        System.out.println("А площадь " + df.format(rectangle.area()));

        double sum_Perimetr = 0;
        double sum_Perimetr1 = 0;
        Shapes[] c = {new Rectangle(2, 7), new Circle(5), new Rectangle(2, 4), new Triangle(4, 8, 1), new Triangle(3, 3, 3)};
        for (int i = 0; i < c.length; i++) {
            sum_Perimetr += c[i].perimetr();
        }
        System.out.println("Сумма периметров всех фигур равна " + sum_Perimetr);

    }
}