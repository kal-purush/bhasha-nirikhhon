using OpenQA.Selenium;
using OpenQA.Selenium.Interactions;
using OpenQA.Selenium.Support.PageObjects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DemoProjectForSelenium
{
    public class SearchResultsPage
    {
        [Obsolete("Use newMethod instead", false)]
        public SearchResultsPage()
        {
            PageFactory.InitElements(PropertiesCollection.driver, this);

        }
        [FindsBy(How = How.ClassName, Using = "product_img_link")]
        public IWebElement SearchResult { get; set; }

        public void HoverOnElement()
        {
            Actions action = new Actions(driver);
            action.MoveToElement(webelement).Perform();
        }

    }   
}