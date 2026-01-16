package org.example.homework_nr_7;

public class Main {
    public static void main(String[] args) {
         Person person1 = new Person("Vasea","Cuza Voda");
        Student student1 = new Student("Fedea", "Miron Costin", "Biologic", 2019, 3500);
        Staff staff1 = new Staff("Kolea", "Stefan cel Mare", "middle", 1200);

         System.out.println(person1.to_String());
        System.out.println(student1.to_String());
        System.out.println(staff1.to_String());

        Circle circle1 = new Circle(15);
        System.out.println(" Площадь круга равна "+ circle1.Area());

        Square square1 = new Square(22);
        System.out.println(" Площадь квадрата равна "+ square1.Area());

        Sphere sphere1 =new Sphere(56);
        System.out.println(" Объем шара равен "+ sphere1.Volume());

        Cube cube1 =new Cube(17);
        System.out.println(" Объем куба равен "+cube1.Volume());
    }
}