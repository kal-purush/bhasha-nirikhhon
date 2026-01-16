package orangeHRM;

import com.microsoft.playwright.*;
import com.microsoft.playwright.options.AriaRole;

public class OrangeHRMAddEmployee {
    public static void main(String[] args) {
        try (Playwright playwright = Playwright.create()) {
            Browser browser = playwright.chromium().launch(new BrowserType.LaunchOptions().setHeadless(false));
            Page page = browser.newPage();

            // Navigate to the OrangeHRM login page
            page.navigate("https://opensource-demo.orangehrmlive.com/");

            // Login
            page.locator("input[name='username']").fill("Admin");
            page.locator("input[name='password']").fill("admin123");
            page.locator("button[type='submit']").click();

            // Wait until dashboard is visible
            page.waitForSelector("h6:has-text('Dashboard')");

            // Click on PIM from left sidebar
            page.locator("a[href='/web/index.php/pim/viewPimModule']").click();
            page.waitForSelector("h6:has-text('PIM')");

            // Click on Add Employee button
            page.getByRole(AriaRole.BUTTON, new Page.GetByRoleOptions().setName("Add")).click();
            page.waitForSelector("h6:has-text('Add Employee')");

            // Fill employee details
            page.locator("input[name='firstName']").fill("Johnny");
            page.locator("input[name='middleName']").fill("D.");
            page.locator("input[name='lastName']").fill("Doee");

//            // Capture Employee ID and modify it
//            Locator employeeIdField = page.locator("input.oxd-input[name='employeeId']");
//            page.waitForSelector("input.oxd-input[name='employeeId']");
//            String existingId = employeeIdField.inputValue();
//            String newId = existingId + "1"; // Append "1" to make it unique
//            employeeIdField.fill(newId);

            // Click Save
            page.getByRole(AriaRole.BUTTON, new Page.GetByRoleOptions().setName("Save")).click();


            // Wait for the employee record page to load
            page.waitForSelector("h6:has-text('Personal Details')");

            // Validation
            if (page.isVisible("h6:has-text('Personal Details')")) {
                System.out.println("Employee added successfully!");
            } else {
                System.out.println("Failed to add employee.");
            }

            // Close browser
            browser.close();
        }
    }
}