// Подумать над структурой класса Ноутбук для магазина техники - выделить поля и методы. Реализовать в java.
//        Создать множество ноутбуков.
//        Написать метод, который будет запрашивать у пользователя критерий (или критерии) фильтрации и выведет ноутбуки, отвечающие фильтру. Критерии фильтрации можно хранить в Map. Например:
//        “Введите цифру, соответствующую необходимому критерию:
//        1 - ОЗУ
//        2 - Объем ЖД
//        3 - Операционная система
//        4 - Цвет …
//        Далее нужно запросить минимальные значения для указанных критериев - сохранить параметры фильтрации можно также в Map.
//        Отфильтровать ноутбуки их первоначального множества и вывести проходящие по условиям.

package ru.geekbrains;

import java.util.*;

public class Comp implements Comparable<Laptop> {

    private String brand;
    private String model;
    private double monitor;
    private String cpu;
    private int ram;
    private String gpu;
    private int hdd;
    private int power;
    private String os;
    private String color;
    private double price;

    public Laptop() {
    }

    public Laptop(String brand, String model, double monitor, String cpu, int ram, String gpu, int hdd, int power, String os, String color, double price) {
        this.brand = brand;
        this.model = model;
        this.monitor = monitor;
        this.cpu = cpu;
        this.ram = ram;
        this.gpu = gpu;
        this.hdd = hdd;
        this.power = power;
        this.os = os;
        this.color = color;
        this.price = price;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public double getMonitor() {
        return monitor;
    }

    public void setMonitor(double monitor) {
        this.monitor = monitor;
    }

    public String getCpu() {
        return cpu;
    }

    public void setCpu(String cpu) {
        this.cpu = cpu;
    }

    public int getRam() {
        return ram;
    }

    public void setRam(int ram) {
        this.ram = ram;
    }

    public String getGpu() {
        return gpu;
    }

    public void setGpu(String gpu) {
        this.gpu = gpu;
    }

    public int getHdd() {
        return hdd;
    }

    public void setHdd(int hdd) {
        this.hdd = hdd;
    }

    public int getPower() {
        return power;
    }

    public void setPower(int power) {
        this.power = power;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Laptop laptop = (Laptop) o;
        return Double.compare(laptop.monitor, monitor) == 0 && ram == laptop.ram && hdd == laptop.hdd && power == laptop.power && Double.compare(laptop.price, price) == 0 && Objects.equals(brand, laptop.brand) && Objects.equals(model, laptop.model) && Objects.equals(cpu, laptop.cpu) && Objects.equals(gpu, laptop.gpu) && Objects.equals(os, laptop.os) && Objects.equals(color, laptop.color);
    }

    @Override
    public int hashCode() {
        return Objects.hash(brand, model, monitor, cpu, ram, gpu, hdd, power, os, color, price);
    }

    @Override
    public String toString() {
        return "Laptop{" +
                "brand='" + brand + '\'' +
                ", model='" + model + '\'' +
                ", monitor=" + monitor +
                ", cpu='" + cpu + '\'' +
                ", ram=" + ram +
                ", gpu='" + gpu + '\'' +
                ", hdd=" + hdd +
                ", power=" + power +
                ", os='" + os + '\'' +
                ", color='" + color + '\'' +
                ", price=" + price +
                '}';
    }

    public static String filtrSelection() {
        Map<Integer, String> criteriaMap = new HashMap<>(Map.of(1, "brand", 2, "monitor", 3, "cpu", 4, "ram", 5, "gpu", 6, "hdd", 7, "power", 8, "os", 9, "color", 10, "price"));
        System.out.println("Input criteria from 1 to 10");
        System.out.println("1-Brand, 2-Monitor size, 3-CPU, 4-RAM, 5-GPU, 6-HDD size, 7-Power, 8-OS, 9-Color, 10-Price");
        Scanner scanner = new Scanner(System.in);
        int criteria = scanner.nextInt();
        scanner.close();
        return criteriaMap.get(criteria);
    }

    public static void output(Set<Laptop> laptop, String filtr) {
        for (Laptop item : laptop) {
            if (filtr.equals("brand")) {
                System.out.println(item.brand);
            } else if (filtr.equals("monitor")) {
                System.out.println(item.monitor);
            } else if (filtr.equals("cpu")) {
                System.out.println(item.cpu);
            } else if (filtr.equals("ram")) {
                System.out.println(item.ram);
            } else if (filtr.equals("gpu")) {
                System.out.println(item.gpu);
            } else if (filtr.equals("hdd")) {
                System.out.println(item.hdd);
            } else if (filtr.equals("power")) {
                System.out.println(item.power);
            } else if (filtr.equals("os")) {
                System.out.println(item.os);
            } else if (filtr.equals("color")) {
                System.out.println(item.color);
            } else if (filtr.equals("price")) {
                System.out.println(item.price);
            }
        }
    }

    public static void main(String[] args) {
        Set<Laptop> laptopSet = new HashSet<>();
        laptopSet.add(new Laptop("Asus", "ABC12345", 15.6, "Core I5", 16, "RTX 3060", 512, 95, "Win10", "Black", 11000));
        laptopSet.add(new Laptop("Aсer", "HJKH045", 14, "Ryzen 5", 8, "RTX 3050", 512, 50, "noOS", "Blue", 85000));
        laptopSet.add(new Laptop("Lenovo", "IDE456", 13.3, "Core I3", 8, "Integred", 256, 36, "Win10", "Black", 62300));
        laptopSet.add(new Laptop("Irbis", "NB655", 13.5, "Intel Pentium J3710", 4, "Integrated", 128, 18, "Win10", "Silver", 15900));
        laptopSet.add(new Laptop("Dell", "NB655", 15.6, "Core I5", 8, "Integrated", 256, 41, "Win11", "Silver", 91900));
        laptopSet.add(new Laptop("Dell", "NLO735", 15.6, "Ryzen 3", 8, "Integrated", 256, 41, "Win11", "White", 59900));
        output(laptopSet, filtrSelection());
    }


    @Override
    public int compareTo(Laptop o) {
        return 0;
    }
}