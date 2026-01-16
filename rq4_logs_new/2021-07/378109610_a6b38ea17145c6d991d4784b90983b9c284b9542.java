package com.home.model;

import java.util.Random;
import java.util.Scanner;

public class Computer {
    //завели поля: процессор, оперативка, жёсткий диск, ресурс полных циклов работы (включений/выключений)
    String processor, hdd;
    int ram, resource;

    //сделаем констуктор для создания объекта компьютер (Computer)

    public Computer(String processor, String hdd, int ram, int resource) {
        this.processor = processor;
        this.hdd = hdd;
        this.ram = ram;
        this.resource = resource;
    }

    //добавим методы: описание(вывод всех полей), включить, выключить, при превышении лимита ресурса комп сгорает.

    //метод описания (выводит все поля)
    public void info() {
        System.out.println(this);
    }

    @Override
    public String toString() {
        return "Компьютер: " +
                "процессор='" + processor + "\'" +
                ", жёсткий диск='" + hdd + "\'" +
                ", оперативная память='" + ram + "\'" + " Gb" +
                ", ресурс полных циклов='" + resource + "\'" + " циклов"
                ;
    }

    //метод включения
    public void on() {
        //учитываем условие: "при повтороном включении компьютра, если он сгорел необходимо выдать сообщение "Компьютер сгорел!"
        if (this.resource == 0) {
            burn();
            // если не сгорел учитываем основную логику
        } else {
            Random randomGenerator = new Random();
            //создаём экземпляр класса Random, который будет генерировать число 0 или 1.
            int genNumber = randomGenerator.nextInt(2);
            Scanner keyboard = new Scanner(System.in);
            System.out.print("Внимание! Введите 0 или 1: ");
            int secretCode = keyboard.nextInt();
            //не большая защита от дурака когда пытаются ввести не то что надо
            while (secretCode < 0 || secretCode > 1) {
                System.out.println("Не правильный ввод, давай сново:");
                secretCode = keyboard.nextInt();
            }
            //если введенное вами число не совпадет с рандомным, то компьютер сгорает.
            if (genNumber != secretCode) {
                burn();
            }
        }
    }

    //метод выключения
    public void off() {
        //учитываем цикл, отнимаем -1
        if (this.resource > 0) {
            this.resource--;
            System.out.println("Компьютер выключился");
            //если ресурс меньше 0, то логично - сгорел
        } else {
            burn();
        }
    }

    //метод сгорел
    void burn() {
        //обнуляем ресурс
        this.resource = 0;
        System.out.println("Компьютер сгорел");
    }
}