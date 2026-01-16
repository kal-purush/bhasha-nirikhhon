using Microsoft.Extensions.Logging;
using OpenQA.Selenium;
using OpenQA.Selenium.Support.UI;
using System;

namespace AutomationTests.pages
{
    public abstract class BasePage
    {
        protected const string URL = "centraldispatch.com";
        protected readonly IWebDriver Driver;
        protected readonly WebDriverWait Wait;
        protected readonly ILogger Logger;

        public BasePage(IWebDriver driver, ILogger<BasePage> logger)
        {
            Logger = logger;
            this.Driver = driver;
            Wait = new WebDriverWait(driver, TimeSpan.FromSeconds(20));
        }

        public abstract BasePage OpenPage();

        public abstract BasePage IsPageOpened();

        public IWebElement DriverFindElement(By locator)
        {
            Logger.LogInformation($"Find element by locator : {locator}");
            IWebElement element;
            try
            {
                element = Driver.FindElement(locator);
            }
            catch (NoSuchElementException)
            {
                throw new Exception($"Element with locator : {locator} does not exist or does not have time to download in DOM");
            }

            return element;
        }

        public void Click(By locator)
        {
            IWebElement element = DriverFindElement(locator);
            try
            {
                Wait.Until(d => element.Displayed == true);
            }
            catch (WebDriverTimeoutException)
            {
                throw new Exception($"Element is not displayed to click");
            }

            element.Click();
        }



    }
}