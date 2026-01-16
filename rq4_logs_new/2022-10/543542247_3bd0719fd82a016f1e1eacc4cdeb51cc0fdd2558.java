package com.demoqa;

import com.codeborne.selenide.Condition;
import com.codeborne.selenide.Configuration;
import com.codeborne.selenide.selector.ByText;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.openqa.selenium.By;

import java.io.File;

import static com.codeborne.selenide.Selectors.byText;
import static com.codeborne.selenide.Selectors.byValue;
import static com.codeborne.selenide.Selenide.*;
import static com.codeborne.selenide.Selenide.$;
import static com.codeborne.selenide.Selenide.open;
import static com.codeborne.selenide.files.DownloadActions.click;

public class practiceForm {

    @BeforeAll
    static void configure() {
        Configuration.baseUrl = "https://demoqa.com";
        Configuration.browserSize = "1920x1080";
    }

    @Test
    void fillFormTest() throws InterruptedException {
        open("/automation-practice-form");
        //Zoom out
        zoom(0.5);
        $("#submit").click();
        //Set name
        $("#userName-wrapper #firstName").setValue("Denis");
        //Set surname
        $("#userName-wrapper #lastName").setValue("Kudla");
        //Set email
        $("#userEmail-wrapper #userEmail").setValue("denis@kudla.com");
        //Set gender
        $(By.xpath("//*[@id=\"genterWrapper\"]/div[2]/div[1]")).click();
        //Set phone number
        $("#userNumber-wrapper #userNumber").setValue("7777777777");
        //Set birth date
        $("#dateOfBirthInput").click();
        $(By.xpath("//*[@id=\"dateOfBirth\"]/div[2]/div[2]/div/div/div[2]/div[1]/div[2]/div[1]/select")).click();
        $(byText("July")).click();
        $(By.xpath("//*[@id=\"dateOfBirth\"]/div[2]/div[2]/div/div/div[2]/div[1]/div[2]/div[2]/select")).click();
        $(byText("1994")).click();
        $(By.xpath("//*[@id=\"dateOfBirth\"]/div[2]/div[2]/div/div/div[2]/div[2]/div[5]/div[6]")).click();
        //Set hobbies
        $(By.xpath("//*[@id=\"hobbiesWrapper\"]/div[2]/div[1]")).click();
        $(By.xpath("//*[@id=\"hobbiesWrapper\"]/div[2]/div[2]")).click();
        $(By.xpath("//*[@id=\"hobbiesWrapper\"]/div[2]/div[3]")).click();
        //Upload image
        $("#uploadPicture").uploadFile(new File("/Users/denis/Downloads/1617343839_50-p-oboi-osennii-sad-59.jpeg"));
        //Set adress
        $("#currentAddress").setValue("st.Pushkina 6, bld. Kolotuskina 9, app 42");
        $("#submit").scrollIntoView(true);
        //More adress
        //  Pls help me X_X i've tried all the stuffs
        $(By.xpath("//*[@id=\"submit\"]")).click();


        //Now lets check

        //Student Name
        $(By.xpath("/html/body/div[4]/div/div/div[2]/div/table/tbody/tr[1]/td[2]")).shouldHave(Condition.text("Denis Kudla"));
        //Student Email
        $(By.xpath("/html/body/div[4]/div/div/div[2]/div/table/tbody/tr[2]/td[2]")).shouldHave(Condition.text("denis@kudla.com"));
        //Gender
        $(By.xpath("/html/body/div[4]/div/div/div[2]/div/table/tbody/tr[3]/td[2]")).shouldHave(Condition.text("Male"));
        //Mobile
        $(By.xpath("/html/body/div[4]/div/div/div[2]/div/table/tbody/tr[4]/td[2]")).shouldHave(Condition.text("7777777777"));
        //Date of Birth
        $(By.xpath("/html/body/div[4]/div/div/div[2]/div/table/tbody/tr[5]/td[2]")).shouldHave(Condition.text("29 July,1994"));
        //Subjects
        //  I could't find the way to check this -_-
        //Hobbies
        $(By.xpath("/html/body/div[4]/div/div/div[2]/div/table/tbody/tr[7]/td[2]")).shouldHave(Condition.text("29 July,1994"));
        //Picture
        $(By.xpath("/html/body/div[4]/div/div/div[2]/div/table/tbody/tr[8]/td[2]")).shouldHave(Condition.text("1617343839_50-p-oboi-osennii-sad-59.jpeg"));
        //Address
        $(By.xpath("/html/body/div[4]/div/div/div[2]/div/table/tbody/tr[9]/td[2]")).shouldHave(Condition.text("st.Pushkina 6, bld. Kolotuskina 9, app 42"));

        //Close tab
        $("#closeLargeModal").click();

    }
}