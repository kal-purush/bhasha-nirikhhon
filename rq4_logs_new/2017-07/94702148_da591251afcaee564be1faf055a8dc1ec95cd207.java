package com.example.pages;

        import org.openqa.selenium.WebDriver;
        import org.openqa.selenium.WebElement;
        import org.openqa.selenium.support.FindBy;
        import org.openqa.selenium.support.PageFactory;
        import org.openqa.selenium.support.ui.Select;

public class Create_Page {
    private final WebDriver driver;

    @FindBy(id = "issue_subject")
    private WebElement issueSubject;

    @FindBy(id = "issue_description")
    private WebElement issueDescription;

    @FindBy(id = "issue_tracker_id")
    private WebElement tracker;

    @FindBy(id = "issue_assigned_to_id")
    private WebElement assignTo;

    @FindBy(xpath = "//label[16]/input")
    private WebElement watcher;

    @FindBy(id = "issue_done_ratio")
    private WebElement doneRatio;

    @FindBy(name = "commit")
    private WebElement commitButton;

    @FindBy(linkText = "Update")
    private WebElement updateButton;

    @FindBy(css = "div.flash.notice")
    private WebElement editFlashMessage;

    @FindBy(id = "issue_priority_id")
    private WebElement priorityId;

    @FindBy(id = "time_entry_activity_id")
    private WebElement activityId;

    @FindBy (xpath = "//div[7]/form/input[2]")
    private WebElement editCimmitButton;

    public Create_Page(WebDriver driver) {
        this.driver = driver;
        PageFactory.initElements(driver, this);
    }

    public Create_Page clickUpdateButton() {
        updateButton.click();
        return new Create_Page(driver);
    }

    public Create_Page clickEditCommitButton() {
        editCimmitButton.click();
        return new Create_Page(driver);
    }

    public Boolean successfulUpdate() {
        return editFlashMessage.getText().contains("Successful update.");
    }

    public EditPage createIssue(String description, String subject, String trackerText, String watcherId, String AssignText) {
        issueDescription.sendKeys(description);
        issueSubject.sendKeys(subject);
        new Select(assignTo).selectByVisibleText(AssignText);
        new Select(tracker).selectByVisibleText(trackerText);
        watcher.click();
        commitButton.click();
        return new EditPage(driver);
    }
    public EditPage UpdateIssue(String ratio, String priority, String activity) {
        new Select(doneRatio).selectByVisibleText(ratio);
        new Select(activityId).selectByVisibleText(activity);
        new Select(priorityId).selectByVisibleText(priority);
        editCimmitButton.click();
        return new EditPage(driver);
    }
}