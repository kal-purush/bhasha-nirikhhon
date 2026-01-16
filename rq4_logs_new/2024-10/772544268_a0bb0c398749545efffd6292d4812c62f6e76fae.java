package org.shikshalokam.backend.ep;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.shikshalokam.backend.PropertyLoader;
import org.testng.Assert;
import org.testng.annotations.Test;


/*
Validating the Login Scenarios in this below class
 */
public class TestElevateLogin extends ElevateProjectBaseTest {
    static final Logger logger = LogManager.getLogger(TestElevateLogin.class);

    @Test(description = "Validating the login with correct credentials")
    public void testLoginWithValidCredentials() {
        //Taking the response from the login method using method calling
        ElevateProjectBaseTest.response  = loginToElevate(PropertyLoader.PROP_LIST.getProperty("elevate.login.contentcreator"), PropertyLoader.PROP_LIST.getProperty("elevate.login.contentcreator.password"));
        ElevateProjectBaseTest.response.prettyPrint();
        Assert.assertEquals(ElevateProjectBaseTest.response.getStatusCode(), 200, "User logged in successfully.");
        Assert.assertEquals(ElevateProjectBaseTest.response.jsonPath().getString("message"), "User logged in successfully.");
        //Printing responce
        logger.info("Validation with correct credentials are verified");
    }

    @Test(description = "Validating the login with incorrect credentials")
    public void testLoginWithInvalidCredentials() {
        ElevateProjectBaseTest.response = loginToElevate("manju@guerrillamail.info", "Password###");
        ElevateProjectBaseTest.response.prettyPrint();
        Assert.assertEquals(ElevateProjectBaseTest.response.getStatusCode(), 400, "Incorrect email/password :- Enter correct email/password");
        Assert.assertEquals(ElevateProjectBaseTest.response.jsonPath().getString("message"), "Please enter the correct login ID and Password.");
        logger.info("Validations with incorrect credentials are verified");
    }

    @Test(description = "Validating the login with empty login fields")
    public void testLoginWithEmptyFields() {
        ElevateProjectBaseTest.response = loginToElevate("manju@guerrillamail.info", "");
        ElevateProjectBaseTest.response.prettyPrint();
        Assert.assertEquals(ElevateProjectBaseTest.response.getStatusCode(), 422, "email/password field is empty");
        Assert.assertEquals(ElevateProjectBaseTest.response.jsonPath().getString("message"), "Validation failed, Entered data is incorrect!");
        logger.info("Validations with login fields empty are verified");
    }
}
