using System;
using NUnit.Framework;
using OpenQA.Selenium.Support.PageObjects;
using SeleniumTestFramework;
using SeleniumTestFramework.Pages;

namespace DEV_10
{
    [TestFixture]
    public class RouteSelectionTest
    {
        private  Browser _browser;
        private static readonly TimeSpan DefaultTimeSpan = TimeSpan.FromSeconds(10);
        
        [SetUp]
        public void SetUp()
        {
            if (_browser == null || _browser.Initialised == false)
            {
                _browser = new Browser();
                _browser.Initialize();
            }
            _browser.WebDriver.Manage().Window.Maximize();
            _browser.WebDriver.Navigate().GoToUrl("https://poezd.rw.by");   
        }
        
        [TearDown]
        public void After()
        {
            _browser.Quit(); 
        }

        [Test]
        public void PositiveTest()
        {
            var mainPage = new MainPage(_browser.WebDriver,DefaultTimeSpan);
            PageFactory.InitElements(_browser.WebDriver, mainPage);
            mainPage.WaitUntil(mainPage.BtnPageRouteSelection).Click();
            
            Assert.True(_browser.WebDriver.Url.Contains("/rp/schedule"),
                "The \"Open route selection page\" button on the main page does not work.");
            
        }
    }
}