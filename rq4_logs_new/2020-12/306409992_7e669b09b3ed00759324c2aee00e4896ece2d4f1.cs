using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace WebAppPatientTests.EndToEndTests
{
    public class CreateFeedbackText : IDisposable
    {
        private readonly IWebDriver driver;
        private Pages.CreateFeedbackPage createFeedbackPage;

        public CreateFeedbackText()
        {
            ChromeOptions options = new ChromeOptions();
            options.AddArguments("start-maximized");            // open Browser in maximized mode
            options.AddArguments("disable-infobars");           // disabling infobars
            options.AddArguments("--disable-extensions");       // disabling extensions
            options.AddArguments("--disable-gpu");              // applicable to windows os only
            options.AddArguments("--disable-dev-shm-usage");    // overcome limited resource problems
            options.AddArguments("--no-sandbox");               // Bypass OS security model
            options.AddArguments("--disable-notifications");    // disable notifications

            driver = new ChromeDriver(options);

            createFeedbackPage = new Pages.CreateFeedbackPage(driver);
            createFeedbackPage.Navigate();
            Assert.Equal(driver.Url, Pages.CreateFeedbackPage.URI);
            Assert.True(createFeedbackPage.TextElementDisplayed());
            Assert.True(createFeedbackPage.PublishedCheckboxDisplayed());
            Assert.True(createFeedbackPage.AnonyimusCheckBoxDisplayed());
            Assert.True(createFeedbackPage.SubmitButtonDisplayed());

        }

        public void Dispose()
        {
            driver.Quit();
            driver.Dispose();
        }

        [Fact]
        public void TestInvalidText()
        {
            createFeedbackPage.InsertText("");
            createFeedbackPage.EnablePublishedCheckbox("true");
            createFeedbackPage.WaitForButton();
            createFeedbackPage.SubmitForm();
            createFeedbackPage.WaitForAlertDialog();
            Assert.Equal(createFeedbackPage.GetDialogMessage(), Pages.CreateFeedbackPage.InvalidMessage);
        }

        [Fact]
        public void TestValidText()
        {
            createFeedbackPage.InsertText("Svidja mi se usluga.");
            createFeedbackPage.EnablePublishedCheckbox("true");
            createFeedbackPage.WaitForButton();

            createFeedbackPage.SubmitForm();
            createFeedbackPage.WaitForAlertDialog();
            Assert.Equal(createFeedbackPage.GetDialogMessage(), Pages.CreateFeedbackPage.ValidMessage);

        }



    }
}