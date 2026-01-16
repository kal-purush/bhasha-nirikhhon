using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OpenQA.Selenium;
using OpenQA.Selenium.Firefox;
using OpenQA.Selenium.Support.PageObjects;

namespace SpecflowTraining
{
    public class AmazonSearchHomePage :DriverConfig
    {
        public AmazonSearchHomePage() : base()
         {
             
             PageFactory.InitElements(Driver, this);

         }
        public AmazonSearchHomePage GoTo(string homeUrl = "http://www.amazon.co.uk")
        {
            Driver.Navigate().GoToUrl(homeUrl);
            return this;
        }
    }
}