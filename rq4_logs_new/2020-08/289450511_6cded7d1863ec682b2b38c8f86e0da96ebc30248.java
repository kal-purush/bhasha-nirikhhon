package com.company.model;

import java.util.Date;

/**
 @author Evgeniy Zhurenko
 */

public class Ticket {

    private String name;                                // Имя пассажира
    private String surname;                             // Фамилия пассажира
    private PassengerWagoon.TypeWagoon typeWagoon;      // Тип вагона
    private Date dateArrival;                           // Дата прибытия
    private Date dateDeparture;                         // Дата отправления
    private boolean bed;                                // Постель
    private TeaCoffee drink;                            // Напитки, которые пассажиру положены по билету
    private Luggage baggage;                            // Багаж, который берет с собой пассажир

    // Конструктор
    public Ticket() {
    }

    // Конструктор с параметрами

    public Ticket(String name, String surname, PassengerWagoon.TypeWagoon typeWagoon, Date dateArrival,
                  Date dateDeparture, boolean bed, TeaCoffee drink, Luggage baggage) {
        this.name = name;
        this.surname = surname;
        this.typeWagoon = typeWagoon;
        this.dateArrival = dateArrival;
        this.dateDeparture = dateDeparture;
        this.bed = bed;
        this.drink = drink;
        this.baggage = baggage;
    }

    // Геттеры и сеттеры на параметры класса Ticket
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSurname() {
        return surname;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }

    public PassengerWagoon.TypeWagoon getTypeWagoon() {
        return typeWagoon;
    }

    public void setTypeWagoon(PassengerWagoon.TypeWagoon typeWagoon) {
        this.typeWagoon = typeWagoon;
    }

    public Date getDateArrival() {
        return dateArrival;
    }

    public void setDateArrival(Date dateArrival) {
        this.dateArrival = dateArrival;
    }

    public Date getDateDeparture() {
        return dateDeparture;
    }

    public void setDateDeparture(Date dateDeparture) {
        this.dateDeparture = dateDeparture;
    }

    public boolean bed() {
        return bed;
    }

    public void setBed(boolean bed) {
        this.bed = bed;
    }

    public TeaCoffee getDrink() {
        return drink;
    }

    public void setDrink(TeaCoffee drink) {
        this.drink = drink;
    }

    public Luggage getBaggage() {
        return baggage;
    }

    public void setBaggage(Luggage baggage) {
        this.baggage = baggage;
    }

    // Метод, который конвертирует булинговое значение поля bed в строковое

    public String isBed(boolean bed){
        if(bed){
            return "Да";
        }
        return "Нет";
    }

    // Переопределенный метож toString

    @Override
    public String toString() {
        return "Билет выдан на : \n" +
                "Фамилия : " + surname + "\n" +
                "Имя : " + name + "\n" +
                "Вагон : " + typeWagoon + "\n" +
                "Отправление : " + dateDeparture + "\n" +
                "Прибытие : " + dateArrival + "\n" +
                "По билету выдается : " + "постель " + isBed(bed) + "; напитки " + drink.getTeaCoffee() + "\n" +
                "Багаж : " + baggage.getLuggage() ;
    }

    // Встроенный enum напитков, которые могут быть приобретены пассажиром вместе с билетом

    public enum TeaCoffee{
        NO_DRINKS("Нет"),
        TEA("Чай"),
        COFFEE("Коффе");

        private String teaCoffee;

        // Конструктор enum напитков

        TeaCoffee(String teaCoffee) {
            this.teaCoffee = teaCoffee;
        }

        // Геттер параметра enum

        public String getTeaCoffee() {
            return teaCoffee;
        }
    }

    // Встреннай enum багажа, которые может провезти с собой пассажир согласно купленного билета

    public enum Luggage {
        NO_LUGGAGE("Ручная кладь до 5 кг"),
        LIGHT_LUGGAGE("Легкая кладь от 5 до 10 кг"),
        MEDIUM_LUGGAGE("Средняя кладь от 10 до 20 кг"),
        HEAVY_LUGGAGE("Тяжелая кладь от 20 кг");

        private String luggage;

        // Конструктор enum напитков

        Luggage(String luggage) {
            this.luggage = luggage;
        }

        // Геттер параметра enum

        public String getLuggage() {
            return luggage;
        }
    }
}

//class  Run {
//    public static void main(String[] args) {
//        Ticket ticket = new Ticket();
//        ticket.setName("Evgeniy");
//        ticket.setSurname("Zhurenko");
//        ticket.setTypeWagoon(PassengerWagoon.TypeWagoon.KYPE);
//        ticket.setDateDeparture(new Date());
//        ticket.setDateArrival(new Date(ticket.getDateDeparture().getTime() + 86400000L));
//        ticket.setBed(false);
//        ticket.setDrink(Ticket.TeaCoffee.NO_DRINKS);
//        ticket.setBaggage(Ticket.Luggage.MEDIUM_LUGGAGE);
//
//        System.out.println(ticket.toString());
//    }
//}
