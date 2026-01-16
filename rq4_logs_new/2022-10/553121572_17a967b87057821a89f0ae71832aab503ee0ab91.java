public class Car {
    String brand;
    String model;
    double engineVolume;
    String color;
    int productionYear;
    String productionCountry;

    Car(String brand, String model, double engineVolume, String color, int productionYear, String productionCountry) {
        this.brand = brand;
        this.model = model;
        this.engineVolume = engineVolume;
        this.color = color;
        this.productionYear = productionYear;
        this.productionCountry = productionCountry;
    }

    void characteristics() {
        System.out.println(brand + " " + model + ". Объём двигателя - " + engineVolume + " л. " + "Цвет кузова - " + color + ". Год выпуска - " + productionYear + ". Сборка - " + productionCountry);
    }

}