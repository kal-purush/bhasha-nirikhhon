using OpenQA.Selenium;
using OpenQA.Selenium.Interactions;
using OpenQA.Selenium.Support.UI;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using excel = Microsoft.Office.Interop.Excel;

namespace FindingBrokenImages
{
    class FunctionLibrary
    {




        public static void clickAction(IWebDriver driver, string LocaterValue, string LocaterType)
        {
            if (LocaterType == "id")
            {
                driver.FindElement(By.Id(LocaterValue)).Click();
            }
            if (LocaterType == "xpath")
            {
                driver.FindElement(By.XPath(LocaterValue)).Click();
            }


        }

        public static void TypeAction(IWebDriver driver, string LocaterValue, string LocaterType, string Value)
        {
            if (LocaterType == "id")
            {
                driver.FindElement(By.Id(LocaterValue)).Clear();
                driver.FindElement(By.Id(LocaterValue)).SendKeys(Value);

            }
            if (LocaterType == "xpath")
            {
                driver.FindElement(By.XPath(LocaterValue)).Clear();
                driver.FindElement(By.XPath(LocaterValue)).SendKeys(Value);
            }
        }

        public static void MouseOver(IWebDriver driver, string LocaterValue)

        {
            IWebElement element = driver.FindElement(By.XPath(LocaterValue));

            ((IJavaScriptExecutor)driver).ExecuteScript("arguments[0].scrollIntoView(); ", element);

            Actions action = new Actions(driver);

            action.MoveToElement(element).Perform();


        }


        public static void waitForElement(IWebDriver driver, string Locatervalue)

        {

            WebDriverWait wait = new WebDriverWait(driver, TimeSpan.FromMinutes(1));

            wait.Until(SeleniumExtras.WaitHelpers.ExpectedConditions.ElementIsVisible(By.XPath(Locatervalue)));


        }

        public static void screenShot(IWebDriver driver)
        {

            string imgName = DateTime.Now.ToString("dd/MM/yyyy-HH-mm-ss");


            Screenshot ss = ((ITakesScreenshot)driver).GetScreenshot();

            ss.SaveAsFile("D:\\ISLproject\\ScreenShots\\" + imgName + ".png");

        }

       public static string ReadDataExcel(int S, int i, int j)
        {
            excel.Application xlapp = new excel.Application();

            excel.Workbook xlworkbook = xlapp.Workbooks.Open(@"D:\FindingBrokenImages\TestInput.xlsx");

            excel._Worksheet xlworksheet = xlworkbook.Sheets[S];

            excel.Range xlrange = xlworksheet.UsedRange;

            string data = xlrange.Cells[i][j].value2;

            return data;
         

        }

        public static void contextClick(IWebDriver driver, string LocaterValue)
        {
            IWebElement element = driver.FindElement(By.XPath(LocaterValue));

            Actions action = new Actions(driver);

            action.ContextClick(element).Perform();

            action.SendKeys(Keys.ArrowRight).Perform();

            action.SendKeys(Keys.Enter).Perform();



        }

        public static void CtrlClick(IWebDriver driver, string Locatervalue)
        {
            IWebElement element = driver.FindElement(By.XPath(Locatervalue));

            Actions action = new Actions(driver);

            action.MoveToElement(element);

            action.KeyDown(Keys.Control).Perform();

            action.Click().Perform();

        }

        public static string ElementText(IWebDriver driver, string locaterValue, string data)
        {

            string text = driver.FindElement(By.XPath(locaterValue)).Text;


            return text;

        }

      

       
    }
}