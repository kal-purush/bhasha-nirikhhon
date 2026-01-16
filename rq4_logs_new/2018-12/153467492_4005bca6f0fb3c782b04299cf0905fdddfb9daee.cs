using OpenQA.Selenium;
using OpenQA.Selenium.Support.PageObjects;

namespace DEV_9.Pages
{
    /// <summary>
    /// Class LoginPage
    /// Page with login function.
    /// </summary>
    public class PageLogin
    {
        [FindsBy(How = How.XPath, Using = "//*[@id=\"index_email\"]")]
        public IWebElement TxtEmail { get; set; }

        [FindsBy(How = How.XPath, Using = "//*[@id=\"index_pass\"]")]
        public IWebElement TxtPassword { get; set; }

        [FindsBy(How = How.XPath, Using = "//*[@id=\"index_login_button\"]")]
        public IWebElement BtnLogin { get; set; }
    }
}