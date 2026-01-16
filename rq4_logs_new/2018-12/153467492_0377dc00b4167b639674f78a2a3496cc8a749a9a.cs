using System;
using OpenQA.Selenium;

namespace SeleniumTestFramework.Pages
{
    public class BuyTicketPage : Page
    {
        public BuyTicketPage(IWebDriver driver, TimeSpan defaultTimeSpan) : base(driver, defaultTimeSpan) { }
        
        public static readonly string Url = "rp/buyTicket";
    }
}