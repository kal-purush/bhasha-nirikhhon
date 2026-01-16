using System;
using Moq;
using PulsePI.DataContracts;
using FluentAssertions;
using PulsePI.Service.ServiceInterfaces;
using Xunit;

namespace PulseTest.ServiceTests
{
    public class BiometricServiceTests
    {
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
            var response = mockBiometricService.Object.CreateBiometric(biometricData);

            //assert
            Assert.True(response.IsCompletedSuccessfully);
            mockBiometricService.Verify();
        }

    }

}