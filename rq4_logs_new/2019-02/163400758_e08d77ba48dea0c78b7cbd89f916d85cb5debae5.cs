using OrangeHRM.App.UI.Tests.Pages;
using TechTalk.SpecFlow;

namespace OrangeHRM.App.UI.Tests.Steps
{
    [Binding]
    public class LoginLogoutSteps : LoginTestsSetup
    {

        private LoginPage loginPage = new LoginPage();

        [Given(@"User is on OrangeHRM App Login page")]
        public void GivenUserIsOnOrangeHRMAppLoginPage()
        {
            loginPage.GoToPageUrl();
        }

        [Given(@"User has entered valid login credentials")]
        public void GivenUserHasEnteredLoginCredentials()
        {
            loginPage.EnterValidCredentials();
        }

        [Given(@"User has entered invalid login credentials: (.*), (.*)")]
        public void GivenUserHasEnteredLoginCredentials(string name, string password)
        {
            loginPage.EnterInvalidCredentials(name, password);
        }

        [When(@"User press login button")]
        public void WhenIPressLoginButton()
        {
            loginPage.ClickLoginButton();
        }

        [Then(@"User should be redirected to the dashboard")]
        public void ThenUserShouldBeRedirectedToDashboard()
        {
            loginPage.ValidateLoginSuccess();
        }

        [Then(@"User should failed while login to the application")]
        public void ThenUserShouldFailedLoginToTheApplication()
        {
            loginPage.ValidateUnsuccessfulLogin();
        }
    }
}