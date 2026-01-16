using System;
using OpenQA.Selenium;
using NUnit.Framework;
using OpenQA.Selenium.Chrome;
using OpenQA.Selenium.Support.UI;
using HW1_Selenium.Pages;

namespace HW1_Selenium
{
    public class TestClass
    {
        private IWebDriver _driver; 
        private string _linkToUrl;
        MainPage MainPage;

        [SetUp]
        public void Before()
        {
            var options = new ChromeOptions();
            options.AddArgument("--start-maximized");
            options.AddArgument("--incognito");
            _driver = new ChromeDriver(options);
        }
         
        [Test]
        public void CheckFirstLink()
        {
            _linkToUrl = "https://stylus.ua/";
            _driver.Navigate().GoToUrl(_linkToUrl);
            new WebDriverWait(_driver, TimeSpan.FromSeconds(5)).Until(d => d.Url == _linkToUrl);
            var promoLink = _driver.FindElement(By.XPath("//ul[@class='header-top-menu']/li/a[contains(@href, '/promo/events/')]"));
            Assert.True(promoLink.Text.Contains("Акции"), "Promo Link doesn't contain word 'Акции'");
            _driver.Quit();
        }

         [Test]
        public void ClickAndCheck() 
        {
            _linkToUrl = "https://stylus.ua/";
            _driver.Navigate().GoToUrl(_linkToUrl);
            new WebDriverWait(_driver, TimeSpan.FromSeconds(5)).Until(d => d.Url == _linkToUrl);
            MainPage = new MainPage(_driver);
            MainPage.PromoButton.Click();
            Assert.True(_driver.Url.Contains("/promo/events/"), "Active page is not Promo page");
            _driver.Quit();
        }
    }
}