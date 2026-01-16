package arcadia.pages.ComponentDB.HouseKeeping;

import arcadia.pages.BasePage;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

public class cleanComponentDB  extends BasePage {
    public cleanComponentDB(WebDriver driver) {
        super(driver);
    }
    public void deleteRecordsInComponentDB(WebElement element , String searchString ) throws InterruptedException {
        boolean proceedWithDelete = true;
        while (proceedWithDelete) {
            element.sendKeys(searchString);
            element.sendKeys(Keys.ENTER);
            Thread.sleep(1000);
            boolean isRecordPresent = driver.findElements(By.cssSelector("#allcomponents >tbody >tr[class=\"no-records-found\"]")).size() > 0;
            if(!isRecordPresent){
                proceedWithDelete = false;
            }
            if(proceedWithDelete){
                WebElement selectAll = driver.findElement(By.cssSelector("input[name=\"btSelectAll\"]"));
                selectAll.click();
                WebElement delete = driver.findElement(By.cssSelector("button[name=\"delete\"]"));
                delete.click();
                WebElement deleteConfirm = driver.findElement(By.cssSelector("button[data-bb-handler=\"confirm\"]"));
                deleteConfirm.click();
                for (int i = 0; i < 50; i++) {
                    boolean isModelPresent = driver.findElements(By.cssSelector("div[class=\"bootbox-body\"]")).size() >0;
                    if(isModelPresent){
                        WebElement deleteSuccessBodyOk = driver.findElement(By.cssSelector("button[data-bb-handler=\"ok\"]"));
                        deleteSuccessBodyOk.click();
                        break;
                    }
                    Thread.sleep(3000);
                }
            }
        }
    }

    public void initiateComponentDBHouseKeeping() throws InterruptedException {
        WebElement companyElement = driver.findElement(By.cssSelector("input[placeholder=\"Company\"]"));
        deleteRecordsInComponentDB(companyElement,"testcompany");
        WebElement descriptionElement = driver.findElement(By.cssSelector("input[placeholder=\"Description\"]"));
        deleteRecordsInComponentDB(descriptionElement,"testdescription-");
    }


}