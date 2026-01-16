using System;
using Moq;
using PulsePI.DataContracts;
using FluentAssertions;
using PulsePI.Service.ServiceInterfaces;
using Xunit;

namespace PulseTest.ServiceTests
{
    public class AccountServiceTests
    {
        private Mock<IAccountService> mockAccountService = new Mock<IAccountService>();

        [Fact]
        public void AccountService_Login_Test()
        {
            //arrange 
            var loginData = new LoginData()
            {
                username = "Username",
                password = "Password"
            };

            //act 
            var response = mockAccountService.Object.Login(loginData);

            //assert
            Assert.True(response.IsCompletedSuccessfully);
            mockAccountService.Verify();
        }
       
        [Fact]
        public void AccountService_CreateAccount_Test()
        {
            var createAccData = new CreateAccountData()
            {
                password = "password",
                username = "username",
                firstName = "firstName",
                lastName = "lastName",
                email = "test@email.com"
            };

            var response = mockAccountService.Object.CreateAccount(createAccData);

            Assert.True(response.IsCompletedSuccessfully);
            mockAccountService.Verify();
        }

    }

}