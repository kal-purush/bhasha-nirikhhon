using System;
using System.Linq;
using OpenQA.Selenium;
using OpenQA.Selenium.Edge;
using OpenQA.Selenium.Support.UI;
using S2p.RestClient.Sdk.Infrastructure.Extensions;
using SeleniumExtras.WaitHelpers;

namespace S2p.RestClient.Sdk.IntegrationTests.Mspec
{
    public class KlarnaPaymentPage : IDisposable
    {
        public string LandingUrl { get; }
        private IWebDriver WebDriver { get; set; }
        private EdgeDriverService DriverService { get; set; }
        private WebDriverWait Wait { get; set; }

        public KlarnaPaymentPage(string landingUrl)
        {
            landingUrl.ThrowIfNullOrWhiteSpace(nameof(landingUrl));

            LandingUrl = landingUrl;
        }

        public void Load()
        {
            DriverService = EdgeDriverService.CreateDefaultService();
            DriverService.HideCommandPromptWindow = true;
            WebDriver = new EdgeDriver(DriverService);

            WebDriver.Navigate().GoToUrl(LandingUrl);
            Wait = new WebDriverWait(WebDriver, TimeSpan.FromSeconds(10));
            WebDriver.SwitchTo().Frame(WebDriver.FindElements(By.Id("klarna-credit-main")).First());
            Wait.Until(ExpectedConditions.ElementExists(By.Id("payment-selector")));
            WebDriver.SwitchTo().DefaultContent();
        }

        public void BuyButtonClick()
        {
            const string buyXpath = "//*[@class='buyButton']";
            Wait.Until(ExpectedConditions.ElementExists(By.XPath(buyXpath)));
            WebDriver.FindElement(By.XPath(buyXpath)).Click();
            Wait.Until(ExpectedConditions.UrlContains("authorized"));
        }

        public void Dispose()
        {
            if (WebDriver != null)
            {
                WebDriver.Dispose();
                WebDriver = null;
            }

            if (DriverService != null)
            {
                DriverService.Dispose();
                DriverService = null;
            }
        }
    }
}