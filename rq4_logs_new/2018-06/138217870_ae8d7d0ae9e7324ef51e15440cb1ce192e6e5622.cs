using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;

namespace BotTheWeb
{
    class Program
    {
        static void Main(string[] args)
        {
            var Options = new ChromeOptions();
            Options.AddArguments("--disable-gpu");

            var chromeDriver = new ChromeDriver();
            chromeDriver.Navigate().GoToUrl("https://www.google.com");

            chromeDriver.FindElementByXPath("//*[@id=\"lst-ib\"]").Click();
            chromeDriver.Keyboard.SendKeys("books");
            chromeDriver.Keyboard.SendKeys(Keys.Enter);
        }
        
    }
}