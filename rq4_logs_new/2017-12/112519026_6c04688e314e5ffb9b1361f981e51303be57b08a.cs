using OpenQA.Selenium.Chrome;
using OpenQA.Selenium;
using NUnit.Framework;
using asp.netAutomationFramework.PageObjects;
using asp.netAutomationFramework.WebDriverAPIWrapper;

namespace asp.netAutomationFramework
{
    public class Tests
    {
        //HomePage homePage = new HomePage();
        BasePage basePage;
        IWebDriver driver;
        WebDriverAPI webAPI;

        [SetUp]
        public void SetUp()
        {
            //homePage.StartChromeDriver();
           basePage = new BasePage(driver);
            driver = basePage.StartChromeDriver();
            
            
        }

        [Test]
        public void GoToHomePage()
        {
            basePage.OpenHomePageByURL();
            HomePage homePage = new HomePage(driver);
            homePage.VerifyHomePageTitle();
            basePage.ClickOnGetStartedLink();
            GetStartedPage getStartedPage = new GetStartedPage(driver);
        }

        [TearDown]
        public void CloseWebdriver()
        {
            webAPI = new WebDriverAPI(driver);
            webAPI.QuitWebBrowser();
            //basePage.CloseWebDriver();
        }
    }
}