import org.openqa.selenium.*;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.time.Duration;
import java.util.List;

public class Element implements WebElement {

    protected WebDriver driver = BaseDriver.initDriver();
    protected WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(20));
    protected Element element;

    public Element findMyElement(By locator) {
        return (Element) driver.findElement(locator);
    }

    @Override
    public void click() {
        System.out.println("Click on the element");
    }

    @Override
    public void submit() {
     wait.until(ExpectedConditions.elementToBeClickable((WebElement) this));
     click();
    }

    @Override
    public void sendKeys(CharSequence... keysToSend) {
        clear();
    }

    @Override
    public void clear() {

    }

    @Override
    public String getTagName() {
        return null;
    }

    @Override
    public String getAttribute(String name) {
        return null;
    }

    @Override
    public boolean isSelected() {
        return false;
    }

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public String getText() {
        return null;
    }

    @Override
    public List<WebElement> findElements(By by) {
        return null;
    }

    @Override
    public WebElement findElement(By by) {
        return null;
    }

    @Override
    public boolean isDisplayed() {
        return false;
    }

    @Override
    public Point getLocation() {
        return null;
    }

    @Override
    public Dimension getSize() {
        return null;
    }

    @Override
    public Rectangle getRect() {
        return null;
    }

    @Override
    public String getCssValue(String propertyName) {
        return null;
    }

    @Override
    public <X> X getScreenshotAs(OutputType<X> target) throws WebDriverException {
        return null;
    }
}