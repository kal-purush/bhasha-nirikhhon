using System.Runtime.InteropServices;
using OpenQA.Selenium;
using OpenQA.Selenium.Support.PageObjects;
using OpenQA.Selenium.Support.UI;

namespace GoogleRecaptcha.AcceptanceTests.PageObjects
{
    public class RecaptchaTestPage : BasePage
    {
        private GoogleRecaptchaIframe _recaptchaIframe;
        private const string TestPageUrl = "http://localhost:64836/index.html";

        public RecaptchaTestPage(IWebDriver driver)
            : base(driver)
        {
            Url = TestPageUrl;
            _recaptchaIframe = new GoogleRecaptchaIframe(driver);
        }

        [FindsBy(How = How.Id, Using = "submit")]
        public IWebElement Submit { get; set; }

        public override IWait<IWebDriver> EnsurePageLoaded()
        {
            var wait = base.EnsurePageLoaded();

            wait.Until(webDriver =>
                       {
                           var iframe = ((IJavaScriptExecutor)driver).ExecuteScript("return document.getElementsByTagName('iframe')");

                           return iframe != null;
                       });

            return wait;
        }

        public void TickRecaptchaCheckBox()
        {
            _recaptchaIframe.TickCheckbox();
        }

        public bool IsGoogleRecaptchaTicked()
        {
            return _recaptchaIframe.IsRecaptchaTicked();
        }
    }
}