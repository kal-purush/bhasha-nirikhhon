package additional;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.support.ui.ExpectedCondition;

public class UrlChanged implements ExpectedCondition<Boolean> {

    private final String currentUrl;
    private final String newUrl;

    @Override
    public Boolean apply(WebDriver driver) {
        System.out.println(currentUrl +" "+newUrl);
        return currentUrl.equals(newUrl);
    }

    public UrlChanged(String currentUrl, String newUrl) {
        this.currentUrl = currentUrl;
        this.newUrl = newUrl;
    }
}