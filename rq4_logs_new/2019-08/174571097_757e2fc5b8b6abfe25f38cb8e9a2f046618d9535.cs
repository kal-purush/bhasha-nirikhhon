using OpenQA.Selenium;
using OpenQA.Selenium.Support.UI;
using System;

namespace Dev9
{
    /// <summary>
    /// The home page.
    /// Signing in is done here.
    /// </summary>
    public class HomePage
    {
        /// <summary>
        /// The WebDriver.
        /// </summary>
        private IWebDriver driver;

        /// <summary>
        /// The username box.
        /// </summary>
        public IWebElement UsernameBox => this.driver.FindElement(By.XPath("//input[@id='mailbox:login']"));

        /// <summary>
        /// The password box.
        /// </summary>
        public IWebElement PasswordBox => this.driver.FindElement(By.XPath("//input[@id='mailbox:password']"));

        /// <summary>
        /// The login button.
        /// </summary>
        public IWebElement LoginButton => this.driver.FindElement(By.XPath("//label[@id='mailbox:submit']"));

        /// <summary>
        /// The login error message.
        /// </summary>
        public IWebElement ErrorMessage => this.driver.FindElement(By.XPath("//div[@id='mailbox:error']"));

        /// <summary>
        /// Initializes a new instance of the class.
        /// </summary>
        /// <param name="driver">
        /// The WebDriver
        /// </param>
        public HomePage(IWebDriver driver)
        {
            this.driver = driver;
        }

        /// <summary>
        /// Method for logging in with provided credentials.
        /// </summary>
        /// <param name="username">
        /// The username.
        /// </param>
        /// <param name="password">
        /// The password.
        /// </param>
        /// <returns>
        /// The Homepage.
        /// </returns>
        public MailInboxPage Login(string username, string password)
        {
            this.UsernameBox.SendKeys(username);
            this.PasswordBox.SendKeys(password);
            this.LoginButton.Click();

            return new MailInboxPage(this.driver);
        }

        /// <summary>
        /// Method for logging in with provided credentials.
        /// For testing wrong/empty inputs only.
        /// </summary>
        /// <param name="username">
        /// The username.
        /// </param>
        /// <param name="password">
        /// The password.
        /// </param>
        /// <returns>
        /// Remains on this HomePage.
        /// </returns>
        public HomePage Login_ExpectingError(string username, string password)
        {
            this.UsernameBox.SendKeys(username);
            this.PasswordBox.SendKeys(password);
            this.LoginButton.Click();
            // Required for wrong inputs.
            new WebDriverWait(this.driver, TimeSpan.FromSeconds(1)).Until(x => this.ErrorMessage.Displayed);

            return this;
        }
    }
}