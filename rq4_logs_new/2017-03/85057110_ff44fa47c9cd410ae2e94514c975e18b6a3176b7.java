package CarShopExtend;

import java.io.Serializable;

public class Seat implements Car, Sellable, Serializable {
    private String model;
    private String color;
    private int horsePower;
    private String countryProduced;
    private double price;

    public Seat(String model, String color, Integer horsePower, String countryProduced, Double price) {
        this.setModel(model);
        this.setColor(color);
        this.setHorsePower(horsePower);
        this.setCountryProduced(countryProduced);
        this.setPrice(price);
    }

    @Override
    public String getModel() {
        return this.model;
    }

    @Override
    public String getColor() {
        return this.color;
    }

    @Override
    public int getHorsePower() {
        return this.horsePower;
    }

    @Override
    public Double getPrice(){
        return this.price;
    }


    public String getCountryProduced(){
        return this.countryProduced;
    }

    private void setCountryProduced(String countryProduced) {
        this.countryProduced = countryProduced;
    }

    private void setModel(String model) {
        this.model = model;
    }

    private void setColor(String color) {
        this.color = color;
    }

    private void setHorsePower(int horsePower) {
        this.horsePower = horsePower;
    }

    private void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString(){
        return String.format("This is %s produced in %s and have %d tires",
                this.getModel(), this.getCountryProduced(), Car.TIRES);
    }
}