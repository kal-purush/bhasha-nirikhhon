package com.epam.brest2019.courses;

import com.epam.brest2019.courses.reader.ConsoleReader;
import com.epam.brest2019.courses.reader.FileReader;
import com.epam.brest2019.courses.reader.impl.BigDecemalConsoleReader;
import com.epam.brest2019.courses.reader.impl.PropertiesFileReader;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;

public class DeliveryCost {

    private static final String QUIT_SYMBOL = "q";
    private final static String PRICE_PER_KG = "delivery_cost/resources/price_per_kg.properties";
    private final static String PRICE_PER_KM = "delivery_cost/resources/price_per_km.properties";
    private final static String PATH = "price_per_kg.properties";



    public static void main(String[] args) {


        BigDecimal weight;
        BigDecimal distance;
        BigDecimal pricePerKg;
        BigDecimal pricePerKm;
        Map<String, BigDecimal> map = null;

        ConsoleReader<BigDecimal> consoleReader = new BigDecemalConsoleReader();
        //FileReader fileReader = new PropertiesFileReader()();
        PropertiesFileReader fileReader = new PropertiesFileReader();

        distance = consoleReader.readData("Enter the distance in kilometers or 'q' for quit");
        weight = consoleReader.readData("Enter the weight in kilograms or 'q' for quit");
        try {
            map = fileReader.readData(PATH);
        } catch (IOException e) {
            e.printStackTrace();
        }
        pricePerKg = fileReader.readPricePerKgFromProperties(map, weight);
        //pricePerKg = fileReader.readPricePerKmFromProperties(distance);
        pricePerKm = fileReader.readPricePerKmFromProperties(distance);

        System.out.println("distance = " + distance);
        System.out.println("weight = " + weight);
        System.out.println("pricePerKg = " + pricePerKg);
        System.out.println("pricePerKm = " + pricePerKm);

        BigDecimal price = weight.multiply(pricePerKg).add(distance.multiply(pricePerKm));
        System.out.println("Price = " + price);


    }

}