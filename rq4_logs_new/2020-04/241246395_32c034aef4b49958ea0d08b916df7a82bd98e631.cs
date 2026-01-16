using PulsePI.Models;
using Xunit;
using System;
using Moq;
using PulsePI.DataContracts;
using FluentAssertions;
using PulsePI.Controllers;
using PulsePI.Service;
using PulsePI.Service.ServiceInterfaces;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using PulsePI.MessageContracts;



namespace PulseTest.ControllerTest{

      public class BiometricDataControllerTests {

        private Mock<IBiometricService> mockBiometricService = new Mock<IBiometricService>();

        [Fact]
        public void BiometricService_CreateBiometric_Test()
        {
            //arrange 
                var biometricData = new BiometricData()
            {
                username = "jack",
                height = "6.0",
                weight = "152",
                sex = "M",
             dob = "1995/04/23"
            };
            //act 
            var result = mockBiometricService.Object.CreateBiometric(biometricData);

            //assert
            Assert.True(result.IsCompletedSuccessfully);
        }
      

      }

}