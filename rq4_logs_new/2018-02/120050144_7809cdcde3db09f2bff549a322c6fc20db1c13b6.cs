using System;
using System.IO;
using OpenQA.Selenium;
using OpenQA.Selenium.Firefox;
using OpenQA.Selenium.Chrome;
using OpenQA.Selenium.IE;
using OpenQA.Selenium.Remote;
using CSharpAutomationFramework.Framework.Helpers;

namespace CSharpAutomationFramework.Framework.Core
{
    public class DriverFactory
    {
        private String webdriversPath;

        //Constructor
        public DriverFactory()
        {
            //Get Root Path
            String workingPath = Generic.GetBasePath();
            webdriversPath = workingPath + "/WebDrivers";
        }

        public IWebDriver GetWebDriver(String browser)
        {
            PlatformID osName = System.Environment.OSVersion.Platform;
            String webDriverType = browser;


            if (webDriverType.ToLower().Equals("firefox") || webDriverType.Length == 0) 
            {
                /*if(osName == PlatformID.Unix || osName == PlatformID.MacOSX) 
                {
                    System.Environment.SetEnvironmentVariable("webdriver.gecko.driver", webdriversPath + "/geckodriver");
                }
                else 
                {
                    System.Environment.SetEnvironmentVariable("webdriver.gecko.driver", webdriversPath + "/geckodriver.exe");
                }*/
                return new FirefoxDriver(webdriversPath);
            }
            else if (webDriverType.ToLower().Equals("chrome")) 
            {
                return new ChromeDriver(webdriversPath);
            }
            else if (webDriverType.ToLower().Equals("ie")){
                System.Environment.SetEnvironmentVariable("webdriver.ie.driver", webdriversPath + "/IEDriverServer32.exe");
                return new InternetExplorerDriver();
            }
            else {
                    Console.WriteLine("Driver type " + webDriverType + " is invalid");
                return null;
            }
        }

        public IWebDriver GetRemoteWebDriver(String URL, ICapabilities dc) 
        {
            return new RemoteWebDriver(new Uri(URL),dc);
        }
    }
}