using System.Net.Configuration;
using System.Runtime.CompilerServices;
using OpenQA.Selenium;

namespace DEV_9.pages
{
    public class LoginPage
    {
        By UsernameLocator = By.XPath("//*[@id=\"index_email\"]");
        By PasswordLocator = By.XPath("//*[@id=\"index_pass\"]");
        By LoginButtonLocator = By.XPath("//*[@id=\"index_login_button\"]");

        public string Login { get; set; }
        public string Password { get; set; }

        private IWebDriver driver;
        
        public LoginPage(IWebDriver driver)
        {
            this.driver = driver;
        }

        public bool TryLogin()
        {
            driver.FindElement(UsernameLocator).Click();
            driver.FindElement(UsernameLocator).SendKeys(Login);
            driver.FindElement(PasswordLocator).SendKeys(Password);
            driver.FindElement(LoginButtonLocator).Click();
            return true;
        }
    }
}