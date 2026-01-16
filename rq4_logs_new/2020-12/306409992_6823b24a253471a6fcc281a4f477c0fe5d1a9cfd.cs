using OpenQA.Selenium;
using OpenQA.Selenium.Support.UI;
using System;
using System.Collections.Generic;
using System.Text;

namespace E2ETests.WebAppPatientE2ETests.Pages
{
    public class PublishFeedbackPage
    {
        private readonly IWebDriver driver;
        public const string URI = "http://localhost:5000/index.html#/patientsFeedbacks";
        private IWebElement PublishFeedbackButton => driver.FindElement(By.Id("publishFeedbackButton"));
        //private IWebElement PublishFeedbackButton => driver.FindElement(By.XPath("//div[@id='careCureHospital']/div/div[@id='allFeedbacks']/div/div[3]/div[2]/div/div[2]/button"));

        public PublishFeedbackPage(IWebDriver driver)
        {
            this.driver = driver;
        }

        public bool PublishFeedbackButtonDisplayed()
        {
            return PublishFeedbackButton.Displayed;
        }

        public void ClickPublishFeedbackButton()
        {
            PublishFeedbackButton.Click();
        }

        public void WaitForButtonClick()
        {
            var wait = new WebDriverWait(driver, new TimeSpan(0, 0, 10));
            wait.Until(SeleniumExtras.WaitHelpers.ExpectedConditions.UrlToBe(PublishedFeedbacksPage.URI));
        }

        public void Navigate() => driver.Navigate().GoToUrl(URI);
    }
}