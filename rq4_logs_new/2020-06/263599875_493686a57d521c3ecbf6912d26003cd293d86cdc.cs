using System;
using System.Collections.Generic;
using System.Text;
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
	public class BarExists_Should
	{
		[TestClass]
		public class CountAllBars_Should
		{
			[TestMethod]
			public async Task ReturnsTrue_WhenBarExists()
			{
				//Arrange
				var options = Utility.GetOptions(nameof(ReturnsTrue_WhenBarExists));

				var bar = new Bar
				{
					Id = new Guid("9bdbf5e7-ad83-415c-b359-9ff5e2f0dedd"),
					Name = "TestBar",
					Phone = "(000) 00-0000",
					Details = "Test Details",
				};

				var mockMapper = new Mock<IBarMapper>();
				mockMapper.Setup(x => x.CreateBarDTO(It.IsAny<Bar>()))
							  .Returns<Bar>(bar => new BarDTO
							  {
								  Id = bar.Id,
								  Name = bar.Name
							  });

				var mockMapperAddress = new Mock<IAddressServices>();
				mockMapperAddress.Setup(x => x.CreateAddressAsync(It.IsAny<AddressDTO>()))
										.ReturnsAsync(new AddressDTO
										{
											Id = new Guid("ff3a5da3-060e-4baa-99d9-372d5701d5f1"),
											CityId = new Guid("320b050b-82f1-494c-9add-91ab28bf98dd"),
											Street = "79-81 MACDOUGAL ST",
										});

				using (var arrangeContext = new CMContext(options))
				{
					await arrangeContext.AddAsync(bar);
					await arrangeContext.SaveChangesAsync();
				};

				//Act / Assert
				using (var assertContext = new CMContext(options))
				{
					var sut = new BarServices(assertContext, mockMapper.Object, mockMapperAddress.Object);

					var result = await sut.BarExists(Guid.Parse("9bdbf5e7-ad83-415c-b359-9ff5e2f0dedd"));
					Assert.IsTrue(result);
				}
			}
		}

	}
}