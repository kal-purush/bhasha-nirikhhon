package cars.entities;

import java.util.*;

public class Car {

    private String make;
    private String model;
    private String year;
    private String engine;
    private String transmission;
    private List<String> values;

    public String getMake() {
        return make;
    }

    public void setMake(String make) {
        this.make = make;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getEngine() {
        return engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
    }

    public String getTransmission() {
        return transmission;
    }

    public void setTransmission(String transmission) {
        this.transmission = transmission;
    }

    public void setNameValues(Map<String, String> map, String name) {

        switch (name) {
            case "makes":
                setMake(map.get(name));
                break;
            case "models":
                setModel(map.get(name));
                break;
            case "years":
                setYear(map.get(name));
                break;
        }

    }

    public List<String> getValues() {

        values = new ArrayList<>();
        values.add(this.getMake());
        values.add(this.getModel());
        values.add(this.getYear());

        return values;
    }


}
