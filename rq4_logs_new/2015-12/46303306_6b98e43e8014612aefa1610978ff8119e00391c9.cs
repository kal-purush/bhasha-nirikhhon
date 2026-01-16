using System;
using NUnit.Framework;
using OpenQA.Selenium;
using OpenQA.Selenium.Firefox;
using OpenQA.Selenium.Support.UI;
using NUnitTestProject1.Pages;
using System.Collections.Generic;

namespace NUnitTestProject1.Tests
{
    [TestFixture]
    public class SystemIntegration
    {
        private string applicationURL = "http://bsia.azurewebsites.net";
        private IWebDriver driver;
        private NavigationBar navBar;

        [SetUp]
        public void Init()
        {
            driver = new FirefoxDriver();
            driver.Manage().Timeouts().ImplicitlyWait(TimeSpan.FromSeconds(3));
            driver.Navigate().GoToUrl(applicationURL);
            navBar = new NavigationBar(driver);
        }

        [Test]
        public void A_Register_User(
            [Values("TestUser1")] string username,
            [Values("Passw0rd!")] string password)
        {
            navBar.ClickRegisterLink();
            RegisterPage registerPage = new RegisterPage(driver);
            registerPage.RegisterUser(username, password, password);
            //Assert user created
        }

        [Test]
        public void B_Register_User_Already_Exists(
            [Values("TestUser1")] string username,
            [Values("Passw0rd!")] string password)
        {
            navBar.ClickRegisterLink();
            RegisterPage registerPage = new RegisterPage(driver);
            registerPage.RegisterUser(username, password, password);
            //Assert error message
        }

        [Test]
        public void C_Register_User_Password_Mismatch(
            [Values("TestUser2")] string username,
            [Values("Passw0rd!")] string password,
            [Values("Passw0rd#")] string wrongPassword)
        {
            navBar.ClickRegisterLink();
            RegisterPage registerPage = new RegisterPage(driver);
            registerPage.RegisterUser(username, password, wrongPassword);
            //Assert error message
        }

        [Test]
        public void D_Login_User(
            [Values("TestUser1")] string username,
            [Values("Passw0rd!")] string password)
        {
            navBar.ClickLoginLink();
            LoginPage loginPage = new LoginPage(driver);
            loginPage.LoginUser(username, password);
            //Assert login
        }

        [Test]
        public void E_Login_User_Wrong_Username(
            [Values("NotRegistered")] string username,
            [Values("Passw0rd!")] string password)
        {
            navBar.ClickLoginLink();
            LoginPage loginPage = new LoginPage(driver);
            loginPage.LoginUser(username, password);
            //Assert error
        }

        [Test]
        public void F_Login_User_Wrong_Password(
            [Values("TestUser1")] string username,
            [Values("WrongPassword")] string password)
        {
            navBar.ClickLoginLink();
            LoginPage loginPage = new LoginPage(driver);
            loginPage.LoginUser(username, password);
            //Assert error
        }

        [Test]
        public void G_Check_Navigation_Links()
        {
            navBar.ClickRegisterLink();
            navBar.ClickLoginLink();

            LoginPage loginPage = new LoginPage(driver);
            loginPage.LoginUser("wysoskyj", "Football2!");

            navBar.ClickHomeLink();
            navBar.ClickCreateLink();
            navBar.ClickEditLink();
            navBar.ClickReportsLink();
        }

        [Test]
        public void H_Create_Inspection(
            [Values("wysoskyj")] string username,
            [Values("Football2!")] string password,
            [Values("104")] string busNumber,
            [Values("Fall")] string season,
            [Values("145256")] string odometer,
            [Values("JK5R4F3")] string tag,
            [Values("Testing")] string notes,
            [Values("Some")] string contractorFirstname,
            [Values("Guy")] string contractorLastname)
        {
            navBar.ClickLoginLink();
            LoginPage loginpage = new LoginPage(driver);
            loginpage.LoginUser(username, password);
            
            navBar.ClickCreateLink();
            CreatePage createPage = new CreatePage(driver);

            createPage.SelectBus(busNumber);
            createPage.SelectSeason(season);
            createPage.ClickGetBusInfo();

            createPage.EnterOdometer(odometer);
            createPage.EnterTag(tag);

            for (int i = 1; i < 8; i++)
            {
                createPage.FillOutInspection(i);
            }

            createPage.EnterNotes(notes);
            createPage.SignInspectionInspector(username);
            createPage.SignInspectionContractor(contractorFirstname, contractorLastname);
            createPage.SubmitInspection();
            //Assert Creation

        }

        [Test]
        public void I_Create_Duplicate_Inspection(
            [Values("wysoskyj")] string username,
            [Values("Football2!")] string password,
            [Values("104")] string busNumber,
            [Values("Fall")] string season)
        {
            navBar.ClickLoginLink();
            LoginPage loginpage = new LoginPage(driver);
            loginpage.LoginUser(username, password);

            navBar.ClickCreateLink();
            CreatePage createPage = new CreatePage(driver);

            createPage.SelectBus(busNumber);
            createPage.SelectSeason(season);
            createPage.ClickGetBusInfo();
            //Assert error message
        }

        [Test]
        public void J_Edit_Inspection(
            [Values("wysoskyj")] string username,
            [Values("Football2!")] string password,
            [Values("104")] string busNumber,
            [Values("Fall")] string season)
        {
            navBar.ClickLoginLink();
            LoginPage loginpage = new LoginPage(driver);
            loginpage.LoginUser(username, password);

            navBar.ClickEditLink();
            EditPage editPage = new EditPage(driver);

            editPage.SelectBus(busNumber);
            editPage.SeasonSelect(season);
            editPage.ClickGetBusInfo();

            editPage.ClickEditButton();

            for (int i = 0; i < 8; i++)
            {
                editPage.FillOutInspection(i);
            }

            editPage.ClickUpdateButton();
            //Assert update
        }

        [Test]
        public void K_Delete_Inspection(
            [Values("wysoskyj")] string username,
            [Values("Football2!")] string password,
            [Values("104")] string busNumber,
            [Values("Fall")] string season)
        {
            navBar.ClickLoginLink();
            LoginPage loginpage = new LoginPage(driver);
            loginpage.LoginUser(username, password);

            navBar.ClickEditLink();
            EditPage editPage = new EditPage(driver);

            editPage.SelectBus(busNumber);
            editPage.SeasonSelect(season);
            editPage.ClickGetBusInfo();

            editPage.ClickDeleteButton();
            //Assert deletion
        }

        [TearDown]
        public void TearDown()
        {
            driver.Quit();
        }
    }
}