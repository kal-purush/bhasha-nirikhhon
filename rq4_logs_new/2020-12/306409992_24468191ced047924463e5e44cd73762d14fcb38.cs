using E2ETests.WebAppPatientE2ETests.Pages;
using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace E2ETests.WebAppPatientE2ETests
{
    public class PostFeedbackTest : IDisposable
    {
        private readonly IWebDriver driver;
        private PostFeedbackPage postFeedbackPage;

        public PostFeedbackTest()
        {
            ChromeOptions options = new ChromeOptions();
            options.AddArguments("start-maximized");          
            options.AddArguments("disable-infobars");          
            options.AddArguments("--disable-extensions");      
            options.AddArguments("--disable-gpu");            
            options.AddArguments("--disable-dev-shm-usage");   
            options.AddArguments("--no-sandbox");               
            options.AddArguments("--disable-notifications");    

            driver = new ChromeDriver(options);

            postFeedbackPage = new PostFeedbackPage(driver);
            postFeedbackPage.Navigate();
            Assert.Equal(driver.Url, PostFeedbackPage.URI);
            Assert.True(postFeedbackPage.TextElementDisplayed());
            Assert.True(postFeedbackPage.PublishedCheckboxDisplayed());
            Assert.True(postFeedbackPage.AnonyimusCheckBoxDisplayed());
            Assert.True(postFeedbackPage.SubmitButtonDisplayed());
        }

        public void Dispose()
        {
            driver.Quit();
            driver.Dispose();
        }

        [Fact]
        public void TestUnsuccessfulPostFeedback()
        {
            postFeedbackPage.InsertText("");
            postFeedbackPage.EnablePublishedCheckbox("true");
            postFeedbackPage.WaitForButton();
            postFeedbackPage.SubmitForm();
            postFeedbackPage.WaitForAlertDialog();
            Assert.Equal(postFeedbackPage.GetDialogMessage(), PostFeedbackPage.InvalidMessage);
        }

        [Fact]
        public void TestSuccessfulPostFeedback()
        {
            postFeedbackPage.InsertText("Sviđa mi se Vaša usluga.");
            postFeedbackPage.EnablePublishedCheckbox("true");
            postFeedbackPage.WaitForButton();
            postFeedbackPage.SubmitForm();
            postFeedbackPage.WaitForAlertDialog();
            Assert.Equal(postFeedbackPage.GetDialogMessage(), PostFeedbackPage.ValidMessage);
        }
    }
}