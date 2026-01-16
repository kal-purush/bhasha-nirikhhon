package com.company.model;

public class PassengerWagoon {
    TypeWagoon typeWagoon;  //тип вагона
    int numberWagoon;       //номер вагина
    int amountSeats;        //колличество мест в вагоне
    int numberSeats;        //номер места в вагоне

    public PassengerWagoon(){  }

    public PassengerWagoon(TypeWagoon typeWagoon, int numberWagoon, int amountSeats, int numberSeats) {
        this.typeWagoon = typeWagoon;
        this.numberWagoon = numberWagoon;
        this.amountSeats = amountSeats;
        this.numberSeats = numberSeats;
    }

    public TypeWagoon getTypeWagoon() {
        return typeWagoon;
    }

    public void setTypeWagoon(TypeWagoon typeWagoon) {
        this.typeWagoon = typeWagoon;
    }

    public int getNumberWagoon() {
        return numberWagoon;
    }

    public void setNumberWagoon(int numberWagoon) {
        this.numberWagoon = numberWagoon;
    }

    public int getAmountSeats() {
        return amountSeats;
    }

    public void setAmountSeats(int amountSeats) {
        this.amountSeats = amountSeats;
    }

    public int getNumberSeats() {
        return numberSeats;
    }

    public void setNumberSeats(int numberSeats) {
        this.numberSeats = numberSeats;
    }

    @Override
    public String toString() {
        return "PassengerWagoon{" +
                "typeWagoon=" + typeWagoon +
                ", numberWagoon=" + numberWagoon +
                ", amountSeats=" + amountSeats +
                ", numberSeats=" + numberSeats +
                '}'+"\n";
    }

    enum TypeWagoon{
        PLATSKART,
        KYPE,
        CW
    }
}