package learningMethodAndClasess;

/**
 * Constructors in Java are special methods that are used to initialize objects when user create operator "new"
 */
public class Constructor {

}

class Car {
    String color;
    int speed;
    double engine;
    int carAge;


    //создали конструктор
    public Car(String color, int speed, double engine, int carAge) {
        this.color = color;
        this.speed = speed;
        this.engine = engine;
        this.carAge = carAge;
    }
   //Перегружаем конструктор
    Car car = new Car("Red", 100, 3.0, 4);
    Car car1 = new Car("Blue", 120, 5.0, 19);
    Car car2 = new Car("Black", 140, 4.0, 5);
    Car car3 = new Car("White", 110, 2.3, 7);
    Car car4 = new Car("Grey", 190, 6.0, 18);


}