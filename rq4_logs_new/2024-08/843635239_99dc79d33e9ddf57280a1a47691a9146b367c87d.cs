using NUnit.Framework;
using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;

namespace BulkyWeb.Tests
{
    [TestFixture]
    public class WebPageTests
    {
        private IWebDriver _driver;

        [SetUp]
        public void SetUp()
        {
            _driver = new ChromeDriver();
        }

        [TearDown]
        public void TearDown()
        {
            _driver.Quit();
            _driver.Dispose();
        }

        [Test]
        public void TestPageTitle()
        {
            _driver.Navigate().GoToUrl("http://localhost:5047");

            string pageTitle = _driver.Title;

            Assert.That(pageTitle, Is.EqualTo("Home Page - BulkyWeb"));
        }
    }
}