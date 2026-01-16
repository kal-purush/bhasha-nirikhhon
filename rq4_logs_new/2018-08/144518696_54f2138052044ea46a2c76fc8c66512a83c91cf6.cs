using OpenQA.Selenium;
using OpenQA.Selenium.Support.PageObjects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DemoProjectForSelenium
{
    public class AccountPage
    {
        [Obsolete("Use newMethod instead", false)]
        public AccountPage()
        {
            PageFactory.InitElements(PropertiesCollection.driver, this);
        }
        //identify the search text box element by the id value as search_query_top
        [FindsBy(How = How.Id, Using = "search_query_top")]
        public IWebElement txtSearch { get; set; }


    }
}