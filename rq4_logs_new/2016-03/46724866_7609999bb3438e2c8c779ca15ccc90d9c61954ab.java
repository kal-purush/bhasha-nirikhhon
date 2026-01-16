package com.selenium.test.pages;

import com.selenium.test.pages.Utils.Utils;
import com.selenium.test.webtestsbase.BasePage;
import com.selenium.test.webtestsbase.WebDriverFactory;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.FindBys;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Created on 20/03/16.
 */
public class CreateAndEditDevicePage extends BasePage {

    public static final int CATEGORY_DROPDOWN = 0;
    public static final int MEDIUM_DROPDOWN = 2;
    public static final int GROUP_DROPDOWN = 1;
    public static final String VALUE_ATTRIBUTE = "value";
    public static final int INTERVAL_DROPDOWN = 3;
    public static final int REMINDER_DROPDOWN = 4;
    public static final int MAINTENANCE_TOGGLE = 0;
    public static final int NOTIFICATION_TOGGLE = 1;
    private static final String PAGE_URL = "http://localhost:9000/#/devices/1/edit";


    @FindBys(@FindBy(css = "i.caret.pull-right"))
    private List<WebElement> dropdowns;

    @FindBys(@FindBy(css = "span.ng-binding.ng-scope"))
    private List<WebElement> dropdownsContent;

    @FindBy(css = "input[name=labels]")
    private WebElement labels;

    @FindBy(css = "input[name='designation']")
    private WebElement designation;

    @FindBy(css = "input[name='serial']")
    private WebElement serialNumber;

    @FindBy(css = "textarea[name='comment']")
    private WebElement comment;

    @FindBy(css = "button.btn.btn-primary.m-r-sm")
    private WebElement save;

    @FindBy(css = "button.btn.btn-default")
    private WebElement clear;

    @FindBys(@FindBy(css = "input[type=checkbox]"))
    private List<WebElement> toggleButtons;

    @FindBy(css = "input[name='email']")
    private WebElement emailField;

    @FindBy(css = "input[name='period']")
    private WebElement maintenaceStart;

    private WebElement category = dropdowns.get(CATEGORY_DROPDOWN);
    private WebElement group = dropdowns.get(GROUP_DROPDOWN);
    private WebElement medium = dropdowns.get(MEDIUM_DROPDOWN);
    private WebElement interval = dropdowns.get(INTERVAL_DROPDOWN);
    private WebElement intervalContent = dropdownsContent.get(INTERVAL_DROPDOWN);
    private WebElement reminder = dropdowns.get(REMINDER_DROPDOWN);
    private WebElement categoryContent = dropdownsContent.get(CATEGORY_DROPDOWN);
    private WebElement groupContent = dropdownsContent.get(GROUP_DROPDOWN);
    private WebElement mediumContent = dropdownsContent.get(MEDIUM_DROPDOWN);
    private WebElement maintenanceToggle = toggleButtons.get(MAINTENANCE_TOGGLE);
    private WebElement notificationToggle = toggleButtons.get(NOTIFICATION_TOGGLE);
    private WebElement reminderContent = dropdownsContent.get(REMINDER_DROPDOWN);


    public CreateAndEditDevicePage(boolean openPageByUrl) {
        super(openPageByUrl);
    }

    /**
     * returns a string representation of the selected category value
     *
     * @return selected category
     */
    public String getCategory() {
        return categoryContent.getText();
    }


    /**
     * selects a category from the category dropdown
     *
     * @param position the position of the dropdown item
     */
    public void setCategory(int position) {
        Utils.clickAndWait(category);
        Utils.clickAndWait(WebDriverFactory
                .getDriver()
                .findElements(By.cssSelector("a.ui-select-choices-row-inner"))
                .get(position));
    }


    /**
     * returns a string representation of the selected medium
     *
     * @return selected medium
     */
    public String getMedium() {
        return mediumContent.getText();
    }

    /**
     * selects a medium from the medium dropdown
     *
     * @param position the position of the dropdown item
     */
    public void setMedium(int position) {
        Utils.clickAndWait(medium);
        Utils.clickAndWait(WebDriverFactory
                .getDriver()
                .findElements(By.cssSelector("a.ui-select-choices-row-inner"))
                .get(position));
    }


    /**
     * returns a string representation of the selected group
     *
     * @return
     */
    public String getGroup() {
        return groupContent.getText();
    }

    /**
     * selects a group from the group dropdown
     *
     * @param position the position of the dropdown item
     */
    public void setGroup(int position) {
        Utils.clickAndWait(group);
        Utils.clickAndWait(WebDriverFactory
                .getDriver()
                .findElements(By.cssSelector("a.ui-select-choices-row-inner"))
                .get(position));

    }

    /**
     * gets a string representation of the label fields value
     *
     * @return
     */
    public String getLabels() {
        return labels.getAttribute(VALUE_ATTRIBUTE);
    }

    public void setLabels(String label) {
        Utils.enterAndWait(labels, label);
    }

    public String getDesignation() {
        return designation.getAttribute(VALUE_ATTRIBUTE);
    }

    public void setDesignation(String designation) {
        Utils.enterAndWait(this.designation, designation);
    }

    public String getSerialNumber() {
        return serialNumber.getAttribute(VALUE_ATTRIBUTE);
    }

    public void setSerialNumber(String serialNumber) {
        Utils.enterAndWait(this.serialNumber, serialNumber);
    }

    public String getComment() {
        return comment.getAttribute(VALUE_ATTRIBUTE);
    }

    public void setComment(String comment) {
        Utils.enterAndWait(this.comment, comment);
    }

    public DevicePage save() {
        Utils.clickAndWait(save);
        return new DevicePage();
    }


    public String getInterval() {
        return intervalContent.getText();
    }

    public void setInterval(int position) {
        Utils.clickAndWait(interval);
        Utils.clickAndWait(WebDriverFactory.getDriver()
                .findElements(By.cssSelector("a.ui-select-choices-row-inner"))
                .get(position));
    }

    public String getReminder() {
        return reminderContent.getText();
    }


    public void setReminder(int position) {
        Utils.clickAndWait(reminder);
        Utils.clickAndWait(WebDriverFactory
                .getDriver()
                .findElements(By.cssSelector("a.ui-select-choices-row-inner"))
                .get(position));
    }

    public void toggleNotification() {
        Utils.clickAndWait(notificationToggle);
    }

    public void toggleMaintenance() {
        Utils.clickAndWait(maintenanceToggle);
    }

    public String getStartDate() {
        return maintenaceStart.getAttribute(VALUE_ATTRIBUTE);
    }

    public void setStartDate(Date date) {
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
        Utils.enterAndWait(maintenaceStart, formatter.format(date));
    }

    public String getEmail() {
        return emailField.getAttribute(VALUE_ATTRIBUTE);
    }

    public void setEmail(String email) {
        Utils.enterAndWait(emailField, email);
    }

    public void clear() {
        Utils.clickAndWait(clear);
    }

    @Override
    protected void openPage() {
        getDriver().get(PAGE_URL);
    }

    @Override
    public boolean isPageOpened() {
        return !dropdowns.isEmpty();
    }


}