using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OpenQA.Selenium.Chrome;
using Xunit;

namespace Axxess.DataDriven.Testing
{
    public class LoginTest
    {
        [Fact]
        public void TestPositiveLogin()
        {
            var driver=new ChromeDriver();
            driver.Navigate().GoToUrl("https://accounts.axxessweb.com/Login");
            driver.FindElementById("Login_UserName").SendKeys("wohe25@gmail.com");
            driver.FindElementById("Login_Password").SendKeys("temp2015");
            driver.FindElementById("Login_Submit").Click();

        }
    }
}