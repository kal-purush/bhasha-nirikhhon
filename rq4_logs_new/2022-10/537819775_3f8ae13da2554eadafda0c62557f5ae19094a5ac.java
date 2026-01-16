package lecture7;

import java.util.Objects;

public class Clothing {
    private String name;
    private int price;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Clothing clothing = (Clothing) o;
        return price == clothing.price && Objects.equals(name, clothing.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, price);
    }

    public Clothing(String name, int price) {
        this.name = name;
        this.price = price;
    }

    public void wash(){
        System.out.println("Cloth is clean");
    }

    public String getName() {
        return name;
    }

    public int getPrice() {
        return price;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "Clothing{" +
                "name='" + name + '\'' +
                ", price=" + price +
                '}';
    }

}