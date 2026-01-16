using System;
using System.Threading.Tasks;
using CM.Data;
using CM.DTOs;
using CM.DTOs.Mappers.Contracts;
using CM.Models;
using CM.Services;
using CM.Services.Contracts;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace CM.ServicesTests.BarServicesTests
{
    [TestClass]
	public class ChangeListing_Should
	{
        [TestMethod]
        public async Task ReturnBarDto_When_ListingUpdated()
        {
            var options = Utility.GetOptions(nameof(ReturnBarDto_When_ListingUpdated));
            await Utility.ArrangeContextAsync(options);

            var mockMapper = new Mock<IBarMapper>();
            mockMapper.Setup(x => x.CreateBarDTO(It.IsAny<Bar>()))
                      .Returns<Bar>(bar => new BarDTO
                      {
                          Id = bar.Id,
                          Name = bar.Name,
                          IsUnlisted = bar.IsUnlisted
                      });

            var mockMapperAddress = new Mock<IAddressServices>();
            mockMapperAddress.Setup(x => x.CreateAddressAsync(It.IsAny<AddressDTO>()))
                            .ReturnsAsync(new AddressDTO
                            {
                                Id = Guid.Parse("ff3a5da3-060e-4baa-99d9-372d5701d5f1"),
                                CityId = Guid.Parse("320b050b-82f1-494c-9add-91ab28bf98dd"),
                                Street = "79-81 MACDOUGAL ST",
                                BarId = Guid.Parse("0999e918-977c-44d5-a5cb-de9559ad822a")
                            });


            var barId = new Guid("0999e918-977c-44d5-a5cb-de9559ad822a"); //unlisted in InMemory

            using var assertContext = new CMContext(options);
            {
                var sut = new BarServices(assertContext, mockMapper.Object, mockMapperAddress.Object);
                var result = await sut.ChangeListingAsync(barId);
                Assert.AreEqual(false, result.IsUnlisted);
            }
        }
    }
}