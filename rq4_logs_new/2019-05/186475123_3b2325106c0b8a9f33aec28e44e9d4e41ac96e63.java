
package br.com.fatecpg.parking.db;

import java.util.Date;


public class ParkingPeriod {
    private long id;
    private String model;
    private String plate;
    private Date beginPeriod;
    private Date endPeriod;

    public ParkingPeriod(long id, String model, String plate, Date beginPeriod) {
        this.id = id;
        this.model = model;
        this.plate = plate;
        this.beginPeriod = beginPeriod;
    }
    private double price;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getPlate() {
        return plate;
    }

    public void setPlate(String plate) {
        this.plate = plate;
    }

    public Date getBeginPeriod() {
        return beginPeriod;
    }

    public void setBeginPeriod(Date beginPeriod) {
        this.beginPeriod = beginPeriod;
    }

    public Date getEndPeriod() {
        return endPeriod;
    }

    public void setEndPeriod(Date endPeriod) {
        this.endPeriod = endPeriod;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
    
    
}