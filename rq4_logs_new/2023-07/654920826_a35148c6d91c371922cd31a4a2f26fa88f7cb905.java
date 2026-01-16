package org.tms.ui.tests;

import io.qameta.allure.Description;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.tms.ui.model.User;
import org.tms.ui.services.LoginPageService;
import org.tms.ui.services.ProjectsPageService;

public class ChatTest extends BaseTest {
    private LoginPageService loginPageService;
    @BeforeClass
    public void setUp() {
        loginPageService = new LoginPageService();
    }

    @Test(description = "Open Chat")
    @Description("Open Chat")
    public void openChatTest() {
        User user = new User(System.getProperty("email"), System.getProperty("password"));
        ProjectsPageService projectsPageService = loginPageService.loginWithUser(user);
        boolean isChatMessagesButtonDisplayed = projectsPageService
                .clickChatButton()
                .isChatMessagesButtonDisplayed();
        Assert.assertTrue(isChatMessagesButtonDisplayed, "Failed to load chat window!");
    }
}