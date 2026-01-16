using PulsePI.Models;
using Xunit;
using System;
using Moq;
using PulsePI.DataContracts;
using FluentAssertions;
using PulsePI.Controllers;
using PulsePI.Service;


namespace PulseTest.ControllerTest{

      public class AccountContollerTests{


 private Mock<I> mockAccountService = new Mock<IAccountService>();
          [Fact]

        public void AccountController_Login_Test()
        {
            //arrange 
            var loginData = new LoginData()
            {
                username = "boyland",
                password = "boyland123"
            };

            //act 
            var response = mockAccountService.Object.Login(loginData);

            //assert
            Assert.True(response.IsCompletedSuccessfully);
            mockAccountService.Verify();
        }

      }

}