package com.pom;

import com.aventstack.extentreports.Status;
import com.base.BaseClass;
import org.openqa.selenium.*;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

import java.io.IOException;
import java.util.List;
import static com.base.BaseClass.logger;

public class GM_FinancialAdminDashboardPage {
    WebDriver driver;
    public GM_FinancialAdminDashboardPage(WebDriver d){
        driver=d;
        PageFactory.initElements(d,this);
    }
    @FindBy (xpath = "//*[text()='Finance Admin']/../../..")
    private WebElement financeAdmin;

    @FindBy (xpath = "//span[contains(text(),'Financial Review')]")
    private WebElement financeReview;

    @FindBy (xpath = "//*[text()='FINANCIAL ARCHIVE']/..")
    private WebElement financeArchiveTab;


    @FindBy (xpath = "//button[@id='Approved']")
    private WebElement btnApprove;

    @FindBy (xpath = "//span[contains(text(),'chevron_right')]")
    private WebElement paging;

    @FindBy (xpath = "//tbody[@class='MuiTableBody-root css-1xnox0e']//td[5]")
    private WebElement financialAppintmentStatus;

    @FindBy (xpath = "//p[contains(text(),'- Edit Status')]")
    private WebElement getIDStatus;

    @FindBy (xpath = "(//label[@id='typo_Statusform_apptstatus']/../following-sibling::div//input)[1]")
    private WebElement appointmentStatus;

    @FindBy (xpath = "//button[@id='btn_Statusform_save']")
    private WebElement saveEditStatus;

    @FindBy (xpath = "//button[@id='btn_Statusform_close']")
    private WebElement closeEditStatus;

    public void navigateFinancialReviewPage() throws InterruptedException , IOException {
        Thread.sleep(2000);
        financeAdmin.click();

        Thread.sleep(2000);
        financeReview.click();

    }
    public boolean navigateFinancialArchivePage() throws InterruptedException,IOException{
        Thread.sleep(2000);
        financeAdmin.click();

        Thread.sleep(2000);
        financeArchiveTab.click();
        Thread.sleep(2000);
      String classVal =  financeArchiveTab.getAttribute("class");
      boolean highlighted =classVal.contains("Mui-selected");
       return  highlighted;

    }

    public WebElement getArchivetab(){

     return financeArchiveTab;
    }
    public void approvingFinancialAppointment(int noOfChecboxRequired,String Status) throws Throwable {


        int boxesChecked=0;
        while(boxesChecked<noOfChecboxRequired) {
            List<WebElement> rows = driver.findElements(By.xpath("//tbody[@class='MuiTableBody-root css-1xnox0e']//td[2]"));
            for(int i=0;i<rows.size();i++) {
                System.out.println(rows.get(i).getText());
                if(rows.get(i).getText().contains(Status)) {
                    Thread.sleep(1000);
                    JavascriptExecutor js = (JavascriptExecutor) driver;
                    String elePath = "(//input[@type='checkbox'])[" + (i + 2 )+ "]";

                    WebElement checkbox=driver.findElement(By.xpath(elePath));
                    js.executeScript("arguments[0].scrollIntoView();",checkbox);
                    js.executeScript("arguments[0].click();", checkbox);
                    boxesChecked++;
                    if(boxesChecked == noOfChecboxRequired)
                        break;

                }

            } // all rows executed
            if(boxesChecked < noOfChecboxRequired) { // for next page
                JavascriptExecutor js = (JavascriptExecutor) driver;
                //WebElement Element = driver.findElement(paging);
                js.executeScript("arguments[0].scrollIntoView();",paging );
                Thread.sleep(2000);
                paging.click();
                Thread.sleep(3000);
            }
        } //end of while
    }
    public void approvingAppointment() throws InterruptedException ,IOException{
        Thread.sleep(3000);
        JavascriptExecutor js = (JavascriptExecutor) driver;
        js.executeScript("window.scrollBy(0,-700)");
        Thread.sleep(2000);
        btnApprove.click();

    }
    public void updateAppointmentStatus(String name) throws InterruptedException, IOException {
        Thread.sleep(2000);
        financeAdmin.click();

        Thread.sleep(2000);
        financeArchiveTab.click();

        Thread.sleep(2000);
        financialAppintmentStatus.click();

        Thread.sleep(2000);
        System.out.println(getIDStatus.getText());
        BaseClass.logger.log(Status.INFO,getIDStatus.getText());
        BaseClass b = new BaseClass();
        logger.addScreenCaptureFromPath(b.takeScreenshotForStep("Editing finance"));


        Thread.sleep(3000);
        appointmentStatus.click();
        Thread.sleep(2000);
        appointmentStatus.sendKeys(BaseClass.datasheet.get(name));
        Thread.sleep(1000);
        appointmentStatus.sendKeys(Keys.TAB);

        Thread.sleep(2000);
        saveEditStatus.click();

        Thread.sleep(2000);
        closeEditStatus.click();
    }
}