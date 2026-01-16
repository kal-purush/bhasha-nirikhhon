using System.IO;
using System.Linq;
using System.Reflection;
using GoogleRecaptcha.AcceptanceTests.PageObjects;
using NUnit.Framework;
using OpenQA.Selenium.Chrome;
using TechTalk.SpecFlow;

namespace GoogleRecaptcha.AcceptanceTests
{
    [Binding]
    public class GoogleRecaptchaVerificationSteps
    {
        private RecaptchaTestPage _testPage;

        public GoogleRecaptchaVerificationSteps()
        {
            _testPage = new RecaptchaTestPage(new ChromeDriver(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)));
        }

        [Given(@"I am on google recaptcha test page")]
        public void GivenIAmOnGoogleRecaptchaTestPage()
        {
            _testPage.Navigate();
            _testPage.EnsurePageLoaded();
        }

        [Given(@"I tick the recaptcha checkbox")]
        public void GivenITickTheRecaptchaCheckbox()
        {
            _testPage.TickRecaptchaCheckBox();
        }

        [When(@"I click on submit")]
        public void WhenIClickOnSubmit()
        {
            ScenarioContext.Current.Pending();
        }

        [Then(@"I should receive recaptcha token")]
        public void ThenIShouldReceiveRecaptchaToken()
        {
            var ticked = _testPage.IsGoogleRecaptchaTicked();
            Assert.IsTrue(ticked);
        }

        [Then(@"I should be redirected to protected page with success")]
        public void ThenIShouldBeRedirectedToProtectedPageWithSuccess()
        {
            ScenarioContext.Current.Pending();
        }
    }
}