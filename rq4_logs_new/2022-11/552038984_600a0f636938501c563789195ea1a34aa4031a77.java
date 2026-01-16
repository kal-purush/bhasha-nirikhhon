package com.tricentis.demowebshop.tests;

import com.tricentis.demowebshop.tests.steps.ClientSteps;
import com.tricentis.demowebshop.tests.pages.MainPage;
import io.qameta.allure.Owner;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;

public class ClientStepsTest extends TestBase {

    ClientSteps clientSteps = new ClientSteps();
    MainPage ui = new MainPage();


    @Owner("kurenkoya")
    @Tag("demowebshop")
    @DisplayName("Checking new user register")
    @Test
    public void checkUserRegister() {
        clientSteps.registerUser();
        clientSteps.checklogin();

    }

    @Tag("demowebshop")
    @DisplayName("Edit registered user data")
    @Test
    public void editUserData() {
        clientSteps.registerUser();
        clientSteps.checklogin();
        clientSteps.updateUserInfo();

    }
    @Tag("demowebshop")
    @DisplayName("Check add product to cart")
    @Test
    public void addProductToCart() {
        clientSteps.registerUser();
        clientSteps.checklogin();
        clientSteps.addToCartTest();


    }

    @Test
    public void loginExistUser() {
        ui.openLoginPage();
    }

}


