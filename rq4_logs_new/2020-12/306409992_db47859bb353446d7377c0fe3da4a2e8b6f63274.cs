using E2ETests.WebAppPatientE2ETests.Pages;
using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace E2ETests.WebAppPatientE2ETests
{
    public class BlockMaliciousPatientsTest : IDisposable
    {
        private readonly IWebDriver driver;
        private BlockMaliciousPatientsPage blockMaliciousPatientsPage;
        private int patientsForBlockingCount = 0;

        public BlockMaliciousPatientsTest()
        {
            ChromeOptions options = new ChromeOptions();
            options.AddArguments("start-maximized");
            options.AddArguments("disable-infobars");
            options.AddArguments("--disable-extensions");
            options.AddArguments("--disable-gpu");
            options.AddArguments("--disable-dev-shm-usage");
            options.AddArguments("--no-sandbox");
            options.AddArguments("--disable-notifications");

            driver = new ChromeDriver(options);

            blockMaliciousPatientsPage = new BlockMaliciousPatientsPage(driver);
            blockMaliciousPatientsPage.Navigate();
            patientsForBlockingCount = blockMaliciousPatientsPage.PatientsForBlockingCount();
            Assert.Equal(driver.Url, BlockMaliciousPatientsPage.URI);
            Assert.True(blockMaliciousPatientsPage.BlockMaliciousPatientButtonDisplayed());
        }

        public void Dispose()
        {
            driver.Quit();
            driver.Dispose();
        }

        [Fact]
        public void TestSuccessfulBlockingMaliciousPatient()
        {
            blockMaliciousPatientsPage.ClickBlockMaliciousPatientButton();
            blockMaliciousPatientsPage.WaitForAlertDialog();
            blockMaliciousPatientsPage.ResolveAlertDialog();

            BlockMaliciousPatientsPage newBlockMaliciousPatientsPage = new BlockMaliciousPatientsPage(driver);
            newBlockMaliciousPatientsPage.Navigate();

            Assert.Equal(patientsForBlockingCount - 1, blockMaliciousPatientsPage.PatientsForBlockingCount());
        }

    }
}