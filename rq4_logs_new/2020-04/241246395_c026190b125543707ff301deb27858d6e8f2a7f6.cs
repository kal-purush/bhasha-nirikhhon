using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using PulsePI.MessageContracts;
using Xunit;
using Moq;
using PulsePI.DataAccess.DaoInterfaces;
using PulsePI.DataContracts;

namespace PulsePI.tests
{
    public class AccountServiceTests
    {
        [Fact]
        public void AccountService_Login_UserNotFound()
        {
            //arrange
            var data = new Mock<IAccountDao>();
            var user = new Task<LoginMessage>("john123", "John", "Cena", "None", "04/21/1975"
                , "cat", "JohnCena@gmail.com");
            data.Setup(m => m.Login(It.IsAny<LoginData>))
                .Returns(user);

            var test = new Task<LoginMessage> Login(data.Object);

            //act


            //assert
        }

    }
}