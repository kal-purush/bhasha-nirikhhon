using OpenQA.Selenium;
using OpenQA.Selenium.Support.PageObjects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UnitTestProject1.PageObjects
{
    public class SearchResultPage : PageBase
    {
        [FindsBy(How = How.Id, Using = "content_plHaveProducts")]
        IWebElement ResultTable;

        [FindsBy(How = How.Id, Using = "J_Filter")]
        IWebElement filters;

        public SearchResultPage(IWebDriver driver) : base(driver)
        {
            PageFactory.InitElements(this.driver, this);{}
        }
    }
}