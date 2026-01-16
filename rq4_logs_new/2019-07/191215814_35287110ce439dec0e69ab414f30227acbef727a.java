package com.epam.brest2019.courses;

import com.epam.brest2019.courses.calculator.Calculator;

import com.epam.brest2019.courses.reader.ConsoleReader;
import com.epam.brest2019.courses.reader.FileReader;
import com.epam.brest2019.courses.scale.ValueScale;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;

@Component
public class DeliveryCostSpring {

    private final static String PRICE_PER_KG_CSV = "price_per_kg.csv";
    private final static String PRICE_PER_KM_CSV = "price_per_km.csv";

    @Value("${weight.message}")
    private String messageWeight;

    @Value("${distance.message}")
    private String messageDistance;

    Calculator calculator;
    FileReader fileReader;
    ValueScale valueScale;
    ConsoleReader consoleReader;

    public DeliveryCostSpring(FileReader fileReader, Calculator calculator, ValueScale valueScale,
                              ConsoleReader consoleReader) {
        this.fileReader = fileReader;
        this.calculator = calculator;
        this.valueScale = valueScale;
        this.consoleReader = consoleReader;
    }

    public static void main(String[] args) throws IOException {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("context.xml");
        DeliveryCostSpring deliveryCostSpring = applicationContext.getBean(DeliveryCostSpring.class);
        deliveryCostSpring.run();

    }


    private void run() throws IOException {
        Map<Integer, BigDecimal> weightMap = null;
        Map<Integer, BigDecimal> distanceMap = null;

        BigDecimal distance = (BigDecimal) consoleReader.readData(messageDistance);
        BigDecimal weight = (BigDecimal) consoleReader.readData(messageWeight);
        try {
            weightMap = fileReader.readData(PRICE_PER_KG_CSV);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            distanceMap = fileReader.readData(PRICE_PER_KM_CSV);
        } catch (IOException e) {
            e.printStackTrace();
        }

        BigDecimal pricePerKg = valueScale.getValue(weightMap, weight);

        BigDecimal pricePerKm = valueScale.getValue(distanceMap, distance);

        System.out.println("distance = " + distance);
        System.out.println("weight = " + weight);
        System.out.println("pricePerKm = " + pricePerKm);
        System.out.println("pricePerKg = " + pricePerKg);

        BigDecimal price = calculator.countDeliveryCost(weight, distance, pricePerKg, pricePerKm);
        System.out.println("Price = " + price);
    }


    }