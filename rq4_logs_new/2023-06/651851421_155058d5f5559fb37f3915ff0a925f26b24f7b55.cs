using DemoStoreSpecflowProject.PageObjects.Locators;
using DemoStoreSpecflowProject.Support;
using OpenQA.Selenium;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DemoStoreSpecflowProject.PageObjects
{
    public  class BasePage
    {
        private IWebDriver driver;
        private BasePageLocators BasePageLocators;
        private SeleniumExtended SeleniumExtended;

        public BasePage(IWebDriver driver)
        { 
           this.driver = driver;
            BasePageLocators = new BasePageLocators();
            SeleniumExtended = new SeleniumExtended(driver,10);
        }

        public void GoToHomePage()
        {
            driver.Navigate().GoToUrl("http://mydemostore.local/");
        }

        public void GoToChartPage()
        {
            driver.Navigate().GoToUrl("http://mydemostore.local/cart/");
        }

        public void GoToMyAccountPage()
        {
            driver.Navigate().GoToUrl("http://mydemostore.local/my-account/");
        }



    }
}