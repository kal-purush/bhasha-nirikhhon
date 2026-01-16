package pages;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import utilities.Driver;

public class EmployeeDetailPage {

    public EmployeeDetailPage(){
        PageFactory.initElements(Driver.getDriver(),this);
    }

    @FindBy(xpath = "(//span[@class = 'oe_menu_text'])[17]")
    public WebElement sideBarEmployeeButton;

    @FindBy(xpath = "//span[@name='name']")
    public WebElement nameOfEmployeeInHeader;

    @FindBy(xpath = "(//button[@class= 'o_dropdown_toggler_btn btn btn-sm dropdown-toggle'])[1]/..")
    public WebElement printDropdownButton;

    @FindBy(xpath = "(//button[@class= 'o_dropdown_toggler_btn btn btn-sm dropdown-toggle'])[2]/..")
    public WebElement attachmentButton;

    @FindBy(xpath = "(//button[@class= 'o_dropdown_toggler_btn btn btn-sm dropdown-toggle'])[3]/..")
    public WebElement actionDropdownButton;

    @FindBy (xpath = "(//a[@data-section='files'])[1]")
    public WebElement latestUploadedFile;

    // nextButton to nav to next Employee
    @FindBy(xpath = "(//button[@class='fa fa-chevron-right btn btn-icon o_pager_next'])[1]")
    public WebElement nextButton;

    // previousButton to nav to previous Employee
    @FindBy(xpath = "(//button[@class='fa fa-chevron-left btn btn-icon o_pager_previous'])[1]")
    public WebElement previousButton;

    // Employee name element (header)
    @FindBy (xpath = "//span[@class='o_field_char o_field_widget o_readonly_modifier o_required_modifier']")
    public WebElement employeeNameInHeader;

    @FindBy(xpath = "/html/body/div[1]/div[2]/div[2]/div/div/div/div[2]/div[1]/button[2]")
    public WebElement logNoteButton;

    @FindBy(xpath = "(//div[@class='o_thread_message_core o_mail_note'])[1]")
    public WebElement lastMessage;

    @FindBy(xpath = "(//i[@title='Mark as Todo'])[1]")
    public WebElement starForLastMessage;

    @FindBy(xpath = "/html/body/div[1]/div[2]/div[2]/div/div/div/div[2]/div[2]/div[1]/div[1]/div[2]/textarea[1]")
    public WebElement logNoteMessageBox;

    @FindBy(xpath = "/html/body/div[1]/div[2]/div[2]/div/div/div/div[2]/div[2]/div[2]/button")
    public WebElement finalLogButton;

    @FindBy (xpath = "//div[@class='o_thread_message_content']/p")
    public WebElement mostRecentLogNote;


    @FindBy (xpath = "//div[@id='o_employee_right']")
    public WebElement organizationChart;

    @FindBy (xpath = "//div[@name='child_ids']")
    public WebElement alertWhenNoRelatedEmployees;

    @FindBy (xpath = "(//a[@class='o_employee_redirect'])[1]")
    public WebElement firstRelatedEmployee;

    @FindBy (xpath = "(//h5/b)[1]")
    public WebElement firstRelatedEmployeeName;

    @FindBy (xpath = "//a[contains(text(),'Work Information')]")
    public WebElement workInformation_tab;

    @FindBy (xpath = "(//div[@class='o_horizontal_separator'])[1]")
    public WebElement contactInformation;

    @FindBy (xpath = "//label[contains(text(),'Work Address')]")
    public WebElement workAddress;

    @FindBy (xpath = "//label[contains(text(),'Work Location')]")
    public WebElement workLocation;

    @FindBy (xpath = "//label[contains(text(),'Work Email')]")
    public WebElement workEmail;

    @FindBy (xpath = " //label[contains(text(),'Work Mobile')]")
    public WebElement workMobile;

    @FindBy (xpath = "//label[contains(text(),'Work Phone')]")
    public WebElement workPhone;

    @FindBy (xpath = "//div[contains(text(),'Position')]")
    public WebElement position;

    @FindBy (xpath = "//label[contains(text(),'Department')]")
    public WebElement department;

    @FindBy (xpath = "//label[contains(text(),'Job Position')]")
    public WebElement jobPosition;

    @FindBy (xpath = "//label[@for='o_field_input_1040']")
    public WebElement manager;

    @FindBy (xpath = "//label[contains(text(),'Coach')]")
    public WebElement coach;

    @FindBy (xpath = "//label[contains(text(),'Working Hours')]")
    public WebElement workingHours;

    @FindBy (xpath = "//button[@title='Send a message']")
    public WebElement sendMessageButton;

    @FindBy (xpath = "//textarea[@class='o_input o_composer_text_field']")
    public WebElement messageBox;

    @FindBy (xpath = "//button[@class='btn btn-sm btn-primary o_composer_button_send hidden-xs']")
    public WebElement sendButtonFinal;

    @FindBy (xpath = "//div[@class='o_thread_message_content']/p")
    public WebElement mostRecentMessage;
    @FindBy(xpath = "//div[@class=\"o_content\"]")
    public WebElement tableContentList;

    @FindBy(xpath = "//div[@class='oe_kanban_details']")
    public WebElement tableForEmployee;

}