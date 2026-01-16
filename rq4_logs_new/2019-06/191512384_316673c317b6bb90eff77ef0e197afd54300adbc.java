package SubscriptionTest.MavenTest;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;


public class BeinSportConnect {

    private static WebElement element = null;

    public static WebElement subscribe (WebDriver driver){

        element = driver.findElement(By.name("Subscribe"));
        return element;


    }

    public static WebElement paket (WebDriver driver){

        element = driver.findElement(By.xpath("/html/body/div[5]/div[1]/div[3]"));
        return element;

    }

    public static WebElement validate (WebDriver driver){

        element = driver.findElement(By.xpath("/html/body/div[5]/div[4]/div/div[2]/div[2]/div[2]/a"));
        return element;

    }
    public static WebElement accountformfirstname (WebDriver driver){

        element = driver.findElement(By.name("FirstName"));
        return element;

    }
    public  static WebElement accountformlastname (WebDriver driver){

        element = driver.findElement(By.name("LastName"));
        return element;

    }
    public  static WebElement accountformEmail (WebDriver driver){

        element = driver.findElement(By.name("EmailOrPhone"));
        return element;

    }

    public static WebElement accountformPassword (WebDriver driver){

        element = driver.findElement(By.name("Password"));
        return element;

    }

    public static WebElement accountformSubmit (WebDriver driver){

        element = driver.findElement(By.cssSelector("html.bc-default.wf-opensans-n3-active.wf-opensans-n4-active.wf-opensans-n5-active.wf-opensans-n6-active.wf-opensans-n7-active.wf-opensans-n8-active.wf-active body.bc-default div.layout-wide div.container-register form#form-register.form-register.account-form div.register-area.form-area div.register div.form-group.position-r input.border-none.bc-subscribe"));
        return element;
    }

    public  static  WebElement emailVerify (WebDriver driver){

        element = driver.findElement(By.cssSelector("html.bc-default.wf-opensans-n3-active.wf-opensans-n4-active.wf-opensans-n5-active.wf-opensans-n6-active.wf-opensans-n7-active.wf-opensans-n8-active.wf-active body.bc-default div#form-messagepopup.modal-me.modal-me-messagepopup div.container div.row.modal-wrapper div.col-xs-12 div.form div.form-header a#close.form-close"));
        return element;
    }

    public static WebElement basketCheck (WebDriver driver){

        element = driver.findElement(By.cssSelector("html.bc-default.wf-opensans-n3-active.wf-opensans-n4-active.wf-opensans-n5-active.wf-opensans-n6-active.wf-opensans-n7-active.wf-opensans-n8-active.wf-active body.bc-default div.layout-wide div.container-onboarding.form-controls form#form-basket div.has-item div.form-group.position-r.clearfix.mt-20-plus div.custom-checkbox label"));
        return element;
    }

    public static WebElement basketPayNow (WebDriver driver){

        element = driver.findElement(By.name("pay-now"));
        return element;
    }

    public static WebElement totalCharge (WebDriver driver){

        element = driver.findElement(By.xpath("//*[@id=\"ncol_ref\"]/tbody/tr[2]/td[2]"));
        return element;
    }

    public static WebElement cardHoldersName (WebDriver driver){

        element = driver.findElement(By.name("Ecom_Payment_Card_Name"));
        return element;
    }

    public static WebElement cardsNumber (WebDriver driver){

        element = driver.findElement(By.name("Ecom_Payment_Card_Number"));
        return element;
    }

    public static WebElement expiryDateMonth (WebDriver driver){

        element = driver.findElement(By.name("Ecom_Payment_Card_ExpDate_Month"));
        return element;
    }

    public static WebElement expiryDateYear (WebDriver driver){

        element = driver.findElement(By.name("Ecom_Payment_Card_ExpDate_Year"));
        return element;
    }

    public static WebElement cardCode (WebDriver driver){

        element = driver.findElement(By.name("Ecom_Payment_Card_Verification"));
        return element;
    }
    public static WebElement confirmPayment (WebDriver driver){

        element = driver.findElement(By.name("payment"));
        return element;
    }

    public static WebElement errorText (WebDriver driver){

        element = driver.findElement(By.cssSelector("html body div#wrapper div#content div.center table.ncoltable1 tbody tr td.ncoltxtc h3"));
        return element;
    }
}