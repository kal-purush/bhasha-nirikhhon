using OpenQA.Selenium;
using NUnit.Framework;
using asp.netAutomationFramework.WebDriverAPIWrapper;

namespace asp.netAutomationFramework.PageObjects
{
    class BasePage
    {
        public BasePage()
        {

        }
        public BasePage(IWebDriver driver)
        { this.driver = driver; }
        private IWebDriver driver;
        

        WebDriverAPI baseAPI = new WebDriverAPI();

#region NavigationBarVars
        By homeLink = By.LinkText("Home");        
        By getStartedLink = By.LinkText("Get Started");
        By learnLink = By.LinkText("Learn");
        By hostingLink = By.LinkText("Hosting");
        By downloadsLink = By.LinkText("Downloads");
        #endregion
#region SearchVars  
        By searchField = By.ClassName("search-input");
        By submitSearchRequest = By.ClassName("search-submit");
        By searchResultsForText = By.Id("search-title");
        #endregion
#region NavigationBarMethods
        public void GoToHomePage()
        {
            baseAPI.NavigateByLinkElement(homeLink);
        }
        public void GoToGetStartedPage()
        {
            baseAPI.NavigateByLinkElement(getStartedLink);
        }
        public void GoToLearnLinkPage()
        {
            baseAPI.NavigateByLinkElement(learnLink);
             }
        public void GoToHostingLinkPage()
        {
            baseAPI.NavigateByLinkElement(hostingLink);
             }
        public void GoToDownloadsPage()
        {
            baseAPI.NavigateByLinkElement(downloadsLink);
            }

        public void GoToDownloads()
        {
            baseAPI.NavigateByLinkElement(downloadsLink);
             }
        //public HomePage OpenHomePage()
        //{
        //    baseAPI.NavigateByURL(basePageURL);
        //    return new HomePage(driver);
        //}
        #endregion
#region SearchMethods
        public BasePage PerformSearch(string searchKeyword)
        {
            driver.FindElement(searchField).SendKeys(searchKeyword);
            driver.FindElement(submitSearchRequest).Click();
            Assert.AreEqual(driver.FindElement(searchResultsForText), searchKeyword);
            return new BasePage(driver);
        }
#endregion
    }
}