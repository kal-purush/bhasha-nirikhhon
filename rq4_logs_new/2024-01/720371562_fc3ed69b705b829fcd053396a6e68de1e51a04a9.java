package com.example.backend.pages.acceptance;

import com.example.backend.pages.login.LoginPage;
import com.example.backend.repositories.UserRepo;
import org.junit.jupiter.api.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
public class AcceptanceNavigationTest {

    @Qualifier("getChromeDriver")
    @Autowired
    private WebDriver driver;


    @Value("${FRONTEND_URL}")
    private String frontendUrl;

    @Autowired
    UserRepo userRepo;

    @Autowired
    PasswordEncoder passwordEncoder;

    // Array of game button classes and corresponding content classes
    private final String[][] gameClasses = {
            {"btnGame1", "contentGame1"},
            {"btnGame2", "contentGame2"},
            {"btnGame3", "contentGame3"},
            {"btnGame4", "contentGame4"},
            {"btnGame5", "contentGame5"},
            {"btnGame6", "contentGame6"}
    };

    @Test
    public void testLoginAndNavigateThroughGamesAndLogout() {
        loginToApplication();
        navigateThroughGames();
        logoutOfApplication();
    }

    private void loginToApplication() {
        driver.get(frontendUrl + "login");
        LoginPage loginPage = PageFactory.initElements(driver, LoginPage.class);
        boolean isRedirected = loginPage.performLoginAndCheckRedirect("user@user.at", "user1234", frontendUrl + "home");
        assertTrue(isRedirected, "The user was not redirected to the home page after login.");
    }

    private void navigateThroughGames() {
        for (String[] gameClass : gameClasses) {
            WebElement gameButton = driver.findElement(By.className(gameClass[0]));
            gameButton.click();
            WebElement gameContent = driver.findElement(By.className(gameClass[1]));
            assertTrue(gameContent.isDisplayed(), "Content for " + gameClass[0] + " is not displayed");
        }
        WebElement scoreboardButton = driver.findElement(By.className("btnScoreboard"));
        scoreboardButton.click();
        WebElement scoreboardContent = driver.findElement(By.className("contentScoreboard"));
        assertTrue(scoreboardContent.isDisplayed(), "Scoreboard content is not displayed");
    }

    private void logoutOfApplication() {
        WebElement logoutButton = driver.findElement(By.className("btnLogout"));
        logoutButton.click();
        WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(10));
        wait.until(ExpectedConditions.urlContains("login"));
        assertTrue(driver.getCurrentUrl().contains("login"), "User is not redirected to login page after logout.");
    }
}