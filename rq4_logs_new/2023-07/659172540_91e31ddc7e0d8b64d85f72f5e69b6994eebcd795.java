package stepikCourse.Java.interfaces;

public class Test1 {
    public static void main(String[] args) {
        Figura f = new Figura();

    }
}

abstract class Figura {
    int kolichestvoStoron;

    abstract void perimetr(); //абстрактные методы без тела

    abstract void ploshad();

    void showInfo() { //неабстрактные метод может быть в абстрактном классе
        System.out.println("Eto figura");
    }


}

class Kvadrat extends Figura {
    int kolichestvoStoron = 4;
    int storona1 = 10;

    public void perimetr() { //реализовали периметр с абстрактного класса
        System.out.println("Perimetr kvadrata = " + 4 * storona1);
    }

    public void ploshad() {
        System.out.println("Ploshad kvadrata = " + storona1 * storona1);

    }

}

class Pryamougolnik extends Figura {
    int kolichestvoStoron = 4;
    int storona1 = 8;
    int storona2 = 5;

    public void perimetr() { //реализовали периметр с абстрактного класса
        System.out.println("Perimetr pryamougolnika = " + 2 * (storona1 + storona2);
    }

    public void ploshad() {
        System.out.println("Ploshad pryamougolnika = " + storona1 * storona2);

    }

}

class Okrujnost extends Figura {

    int kolichestvoStoron = 4;
    int storona1 = 8;
    int storona2 = 5;

    public void perimetr() { //реализовали периметр с абстрактного класса
        System.out.println("Perimetr pryamougolnika = " + 2 * (storona1 + storona2);
    }

    public void ploshad() {
        System.out.println("Ploshad pryamougolnika = " + storona1 * storona2);

    }
}