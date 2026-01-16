package com.cydeo.tests.day09_javafaker_driverUtil;

import com.github.javafaker.Faker;
import org.testng.annotations.Test;

public class JavaFakerPractice {

    @Test
    public void javaFakerTet1 (){

        //create java faker object
        Faker faker = new Faker();

        //Print our random first name using faker object
        System.out.println("faker.name().firstName() = " + faker.name().firstName());

        //print out random last name using fake
        System.out.println("faker.name().lastName() = " + faker.name().lastName());

        //print out full name
        System.out.println("faker.name().fullName() = " + faker.name().fullName());

        System.out.println("faker.numerify(\"###-###-####\") = " + faker.numerify("###-###-####"));

        System.out.println("faker.address().city() = " + faker.address().city());

        System.out.println("faker.address().zipCode() = " + faker.address().zipCode());

        System.out.println("faker.address().fullAddress() = " + faker.address().fullAddress());

        System.out.println("faker.letterify(\"??-??-????\") = " + faker.letterify("??-??-????"));

        System.out.println("faker.bothify(\"#?#?-###???-#??#?\") = " + faker.bothify("#?#?-###???-#??#?"));

        //chuckNorris methods is used to create chuck norris facts
        System.out.println("faker.muhtar().chuckNorris().fact() = " + faker.chuckNorris().fact().replaceAll("Chuck Norris","Muhtar"));
    }

}