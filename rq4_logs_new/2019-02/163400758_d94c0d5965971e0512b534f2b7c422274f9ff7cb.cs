using OrangeHRM.App.UI.Tests.Pages;
using TechTalk.SpecFlow;

namespace OrangeHRM.App.UI.Tests.Steps
{
    [Binding]
    public class LoginSteps : LoginTestsSetup
    {

        private LoginPage loginPage = new LoginPage();

        [Given(@"User is on the OrangeHRM App Login page")]
        public void GivenUserIsOnTheOrangeHRMAppLoginPage()
        {
            loginPage.GoToPageUrl();
        }

        [Given(@"User has entered a valid login credentials")]
        public void GivenUserHasEnteredLoginCredentials()
        {
            loginPage.EnterValidCredentials();
        }

        [Given(@"User has entered an invalid login credentials: (.*), (.*)")]
        public void GivenUserHasEnteredInvalidLoginCredentials(string name, string password)
        {
            loginPage.EnterInvalidCredentials(name, password);
        }

        [When(@"User press a login button")]
        public void WhenIPressALoginButton()
        {
            loginPage.ClickLoginButton();
        }

        [Then(@"User should be redirected to the dashboard page")]
        public void ThenUserShouldBeRedirectedToDashboardPage()
        {
            loginPage.ValidateLoginSuccess();
        }

        [Then(@"User should have failed while login to the application")]
        public void ThenUserShouldHaveFailedLoginToTheApplication()
        {
            loginPage.ValidateUnsuccessfulLogin();
        }
    }
}