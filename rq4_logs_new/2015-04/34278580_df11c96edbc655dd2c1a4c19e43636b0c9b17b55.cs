using OpenQA.Selenium;
using OpenQA.Selenium.Support.PageObjects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UnitTestProject1.PageObjects
{
    public class HomePage : PageBase
    {
        [FindsBy(How = How.CssSelector, Using = "span.flag.t.flag-ar")]
        IWebElement freeShippingButton; 

        [FindsBy(How = How.LinkText, Using="gercho100")]
        protected IWebElement username;

        [FindsBy(How = How.LinkText, Using = "Logout")]
        protected IWebElement logout;

        [FindsBy(How = How.CssSelector, Using = "img[alt='Cool Gadgets at DX']")]
        protected IWebElement logo;

        [FindsBy(How = How.Id, Using = "txtKeyword")]
        protected IWebElement searchField;

        [FindsBy(How = How.Id)]
        IWebElement btnSearch;

        [FindsBy(How = How.Id)]
        IWebElement miniCart;

        [FindsBy(How = How.LinkText)]
        IWebElement Logout;
        
        public HomePage(IWebDriver d) :base(d)
        {
            PageFactory.InitElements(this.driver, this);
        }

        public bool BeInHomePage()
        {
            return freeShippingButton.Displayed && username.Displayed;
        }

        public MyDxPage ClickUserName()
        {
            username.Click();
            return new MyDxPage(this.driver);
        }

        public LoginPage LogOut()
        {
            Logout.Click();
            return new LoginPage(this.driver);
        }
    }
}