package CarShopExtend;

public class Audi implements Rentable{
    private String model;
    private String color;
    private int horsePower;
    private String countryProduced;
    private int minDaysForRent;
    private double pricePerDay;

    public Audi(String model, String color, Integer horsePower, String countryProduced, Integer minDaysForRent, Double pricePerDay) {
        this.setModel(model);
        this.setColor(color);
        this.setHorsePower(horsePower);
        this.setCountryProduced(countryProduced);
        this.setMinDaysForRent(minDaysForRent);
        this.setPricePerDay(pricePerDay);
    }

    public void setModel(String model) {
        this.model = model;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public void setHorsePower(int horsePower) {
        this.horsePower = horsePower;
    }

    public void setCountryProduced(String countryProduced) {
        this.countryProduced = countryProduced;
    }

    public void setMinDaysForRent(int minDaysForRent) {
        this.minDaysForRent = minDaysForRent;
    }

    public void setPricePerDay(double pricePerDay) {
        this.pricePerDay = pricePerDay;
    }

    @Override
    public String getModel(){
        return this.model;
    }
    @Override
    public String getColor(){
        return this.color;
    }
    @Override
    public int getHorsePower(){
        return this.horsePower;
    }
    @Override
    public Integer getMinRentDay(){
        return this.minDaysForRent;
    }
    @Override
    public Double getPricePerDay(){
        return this.pricePerDay;
    }

    public String getCountryProduced(){
        return this.countryProduced;
    }
    @Override
    public String toString(){
        return String.format("This is %s produced in %s and have %d tires",
                this.getModel(), this.getCountryProduced(), Car.TIRES);

    }
}