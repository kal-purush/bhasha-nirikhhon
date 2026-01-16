package com.company.model;

public class Locomotive {
    int capacityLocomotive;     //количество вагонов которые может тянуть локомотив
    int powerLocomotive;        //мощьность локомотива
    int yearIssueLocomotive;    //год выпуска локомотива
    FuelType fuelType;          //тип топлива локомотива

    public Locomotive(){ }

    public Locomotive(int capacityLocomotive, int powerLocomotive, int yearIssueLocomotive, FuelType fuelType) {
        this.capacityLocomotive = capacityLocomotive;
        this.powerLocomotive = powerLocomotive;
        this.yearIssueLocomotive = yearIssueLocomotive;
        this.fuelType = fuelType;
    }

    public int getCapacityLocomotive() {
        return capacityLocomotive;
    }

    public void setCapacityLocomotive(int capacityLocomotive) {
        this.capacityLocomotive = capacityLocomotive;
    }

    public int getPowerLocomotive() {
        return powerLocomotive;
    }

    public void setPowerLocomotive(int powerLocomotive) {
        this.powerLocomotive = powerLocomotive;
    }

    public int getYearIssueLocomotive() {
        return yearIssueLocomotive;
    }

    public void setYearIssueLocomotive(int yearIssueLocomotive) {
        this.yearIssueLocomotive = yearIssueLocomotive;
    }

    public FuelType getFuelType() {
        return fuelType;
    }

    public void setFuelType(FuelType fuelType) {
        this.fuelType = fuelType;
    }

    @Override
    public String toString() {
        return "Locomotive{" +
                "capacityLocomotive=" + capacityLocomotive +
                ", powerLocomotive=" + powerLocomotive +
                ", yearIssueLocomotive=" + yearIssueLocomotive +
                ", fuelType=" + fuelType +
                '}'+"\n";
    }

    public enum FuelType{
        DIESEL,
        ELECTRICITY
    }
}