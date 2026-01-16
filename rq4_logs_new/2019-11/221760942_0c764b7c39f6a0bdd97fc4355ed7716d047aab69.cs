using System;
using FluentAssertions;
using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;
using OpenQA.Selenium.Firefox;
using Xunit;

namespace Algenic.FunctionalTests
{
    public class SampleFunctionalTest : IDisposable
    {
        private readonly IWebDriver _driver;

        public SampleFunctionalTest()
        {
            _driver = new FirefoxDriver(".");
            _driver.Manage().Timeouts().ImplicitWait = TimeSpan.FromSeconds(10);
        }

        public void Dispose()
        {
            _driver.Quit();
            _driver.Dispose();
        }

        [Fact]
        public void IndexTitleTest()
        {
            _driver.Navigate()
                .GoToUrl(@"http://localhost:5000");

            _driver.Title.Should().Be("Home page - Algenic");
        }
    }
}