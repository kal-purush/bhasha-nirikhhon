using System;
using System.Linq;
using OpenQA.Selenium;
using OpenQA.Selenium.Support.UI;

namespace GoogleRecaptcha.AcceptanceTests.PageObjects
{
    public class GoogleRecaptchaIframe
    {
        private IWebDriver driver;

        public GoogleRecaptchaIframe(IWebDriver driver)
        {
            this.driver = driver;
        }

        public void TickCheckbox()
        {
            ExecuteInIframeContext((d) =>
            {
                d.FindElement(By.Id("recaptcha-anchor")).Click();

                var wait = new WebDriverWait(d, TimeSpan.FromSeconds(10));
                wait.Until(webDriver =>
                {
                    return webDriver.FindElement(By.ClassName("recaptcha-checkbox-checked")) != null;
                });
            });

        }
        
        private void ExecuteInIframeContext(Action<IWebDriver> action)
        {
            driver.SwitchTo().Frame(driver.FindElement(By.TagName("iframe")));
            action.Invoke(driver);
            driver.SwitchTo().DefaultContent();
        }


        public bool IsRecaptchaTicked()
        {
            bool result = false;
            ExecuteInIframeContext((d) =>
                            {
                                result  = d.FindElement(By.Id("recaptcha-anchor")).GetAttribute("class").Split(' ').Contains("recaptcha-checkbox-checked");
                            });
            return result;
        }
    }
}