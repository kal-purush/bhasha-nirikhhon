using System;
using DEV_9.pages;
using Microsoft.Win32;
using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;
using OpenQA.Selenium.Support.UI;

namespace DEV_9
{
    /// <summary>
    /// Class Entry point
    /// Using WebDriver, logs in and displays to the console a list of unread messages from all users.
    /// </summary>
    internal class EntryPoint
    {
        /// <summary>
        /// Method Main
        /// Entry point.
        /// </summary>
        public static void Main()
        {
            try
            {
                IWebDriver driver = new ChromeDriver("./DEV-9");
                driver.Manage().Timeouts().ImplicitWait = TimeSpan.FromSeconds(60);
                driver.Manage().Window.Maximize();
                driver.Navigate().GoToUrl("https://vk.com/");
                
                var loginPage = new LoginPage(driver);
                loginPage.Login = "Login";
                loginPage.Password = "Password";
                loginPage.TryLogin();
                
                var feedPage = new FeedPage(driver);
                feedPage.OpenMessagePage();
                
                var messagePage = new MessagePage(driver);
                messagePage.GetUnreadMessages();
                
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
    }
}