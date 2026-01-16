using OpenQA.Selenium;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OpenQA.Selenium.Support.UI;
using OpenQA.Selenium.Interactions;

namespace DemoProjectForSelenium
{

    class SeleniumSetMethods
    {
        //generic Enter Text method to support all text box controls for the application 
        //Enter Text takes three parameters(element, value, type)
        public static void EnterText(IWebDriver driver, string element, string value, string elementtype)
        {
            //driver.FindElement(By.ClassName("login")).Click();

            if (elementtype == "id")
                driver.FindElement(By.Id(element)).SendKeys(value);
            if (elementtype == "name")
                driver.FindElement(By.Name(element)).SendKeys(value);

        }

        //method to do a click operation into a button, checkbox, option etc
        public static void Click(IWebDriver driver, string element, string elementtype)
        {
            Console.WriteLine(element);
            Console.WriteLine(elementtype);
            if (elementtype == "id")
                driver.FindElement(By.Id(element)).Click();
            if (elementtype == "name")
                driver.FindElement(By.Name(element)).Click();
            if (elementtype == "class")
                driver.FindElement(By.ClassName(element)).Click();
            if (elementtype == "xpath")
                driver.FindElement(By.XPath(element)).Click();

        }

        //method to hover over an element
        public static void HoverOnElement(IWebDriver driver, string element, string elementtype)
        {

            Console.WriteLine(element);
            Console.WriteLine(elementtype);
            IWebElement webelement = driver.FindElement(By.ClassName(element));
            
            Actions action = new Actions(driver);
            action.MoveToElement(webelement).Perform();
        }

        //method to select a dropdown control
        public static void SelectDropDown(IWebDriver driver, string element, string value, string elementtype)
        {

        }
    }
}