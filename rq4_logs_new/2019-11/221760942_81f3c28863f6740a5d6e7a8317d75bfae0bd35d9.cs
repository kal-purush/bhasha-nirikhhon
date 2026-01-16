using System;
using System.Collections.Generic;
using System.Linq;
using Algenic.FunctionalTests.Setup;
using FluentAssertions;
using OpenQA.Selenium;
using Xunit;

namespace Algenic.FunctionalTests
{
    public class AdminPanelAccess : BaseFunctionalTest
    {
        public AdminPanelAccess(DriverFixture driverFixture)
            : base(driverFixture) { }

        [Fact]
        public void AccessAdminPanel_RegularUser_AccessDenied()
        {
            var guid = Guid.NewGuid();
            var email = $"{guid}@example.com";
            var password = "123456";

            RegisterUser(email, password);
            LoginAs(email, password);
            _driver.Navigate().GoToUrl(new Uri(new Uri(_indexUrl), "/Admin"));

            _driver.PageSource.Should().Contain("Access denied");
        }

        private void LoginAs(string email, string password)
        {
            var loginUrl = new Uri(new Uri(_indexUrl), "/Identity/Account/Login");
            _driver.Navigate().GoToUrl(loginUrl);

            var loginForm = _driver.FindElements(By.Id("account")).Single();
            FillForm(loginForm, new Dictionary<string, string>
            {
                { "Input_Email", email },
                { "Input_Password", password }
            });
        }

        private void RegisterUser(string email, string password)
        {
            var registerUrl = new Uri(new Uri(_indexUrl), "/Identity/Account/Register");
            _driver.Navigate().GoToUrl(registerUrl);

            var registerForm = _driver.FindElements(By.TagName("form")).Single();
            FillForm(registerForm, new Dictionary<string, string>
            {
                { "Input_Email", email },
                { "Input_Password", password },
                { "Input_ConfirmPassword", password }
            });
        }

        private void FillForm(IWebElement formElement, IDictionary<string, string> fieldValues)
        {
            foreach (var pair in fieldValues)
            {
                var textBox = formElement.FindElement(By.Id(pair.Key));
                textBox.Clear();
                textBox.SendKeys(pair.Value);
            }

            formElement.Submit();
        }
    }
}