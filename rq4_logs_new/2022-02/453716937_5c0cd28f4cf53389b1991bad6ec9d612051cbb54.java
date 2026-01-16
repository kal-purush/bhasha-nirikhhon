import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

public class RegisterPage {

    private final WebDriver driver;

    public RegisterPage(WebDriver driver) {
        this.driver = driver;

        if (!driver.getCurrentUrl().equals("https://organic-shops.ru/users/register.html")) {
            throw new IllegalStateException("This is not a Register page.");
        }
    }
}