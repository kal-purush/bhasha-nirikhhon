using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;

namespace SeleniumTestFramework
{
    public class Browser
    {
        public IWebDriver WebDriver { get; set; }
        public bool Initialised { get; set; }

        private const string ChromeDriverDirectory = "/home/danila";

        public void Initialize()
        {
            WebDriver = new ChromeDriver(ChromeDriverDirectory);
            Initialised = true;
        }

        public  void Quit()
        {
            WebDriver.Quit();
            Initialised = false;
        }
    }
}