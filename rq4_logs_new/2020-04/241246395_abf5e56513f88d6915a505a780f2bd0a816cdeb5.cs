using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using PulsePI.MessageContracts;
using Moq;
using Moq.Protected;
using PulsePI.DataAccess.DaoInterfaces;
using PulsePI.DataContracts;
using PulsePI.Service;
using PulsePI.Controllers;
using PulsePI.Models;


namespace PulseTest
{
    
    public class AccountServiceTests
    {

        public void AccountService_Login_Test()
        {
            //arrange
            var mockLogin = new Mock<LoginData>();

            Action act = () =>
            {
                var test = new AccountService(mockLogin.Object);
                Assert.areEqual("4". test.)
           }
          

            //act


            //assert
        }

        /*
        public void AccountService_CreateAccount_Test()
        {
            //arrange

            //act

            //assert
        }
        */
    }

}