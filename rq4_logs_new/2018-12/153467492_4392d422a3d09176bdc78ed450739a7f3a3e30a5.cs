using System;
using DEV_9.pages;
using Microsoft.Win32;
using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;
using OpenQA.Selenium.Support.UI;

namespace DEV_9
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            try
            {
                IWebDriver driver = new ChromeDriver("./DEV-9");
                driver.Manage().Timeouts().ImplicitWait = TimeSpan.FromSeconds(60);
                driver.Navigate().GoToUrl("https://vk.com/");
                var a = new LoginPage(driver);
                a.Login = "380964881403";
                a.Password = "YCmkf7de";
                a.TryLogin();
                var b =new FeedPage(driver);
                b.OpenMessagePage();
                var c = new MessagePage(driver);
                c.GetUnreadMessages();
                Console.ReadKey();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
    }
}//*[@id="side_bar_inner"]