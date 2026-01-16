using System;
using System.Linq;
using System.Threading.Tasks;
using CM.Data;
using CM.DTOs;
using CM.DTOs.Mappers.Contracts;
using CM.Models;
using CM.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace CM.ServicesTests.RatingServicesTests
{
	[TestClass]
	public class RateBarAsync_Should
	{
        [TestMethod]
        public async Task ThrowsNullReferenceException_When_BarNotFound()
        {
            var options = Utility.GetOptions(nameof(ThrowsNullReferenceException_When_BarNotFound));

            var mockBarMapper = new Mock<IBarMapper>();
            var mockCocktailMapper = new Mock<ICocktailMapper>();

            var input = new BarRatingDTO
            {
                AppUserId = Guid.Parse("b8a3552e-f509-42f0-bed9-7c29c3dfc6b6"),
                BarId = Guid.Parse("5416ceee-839d-43e3-bb85-b292976c353e"), 
                Score = 4,
            };

            var input1 = new BarRatingDTO
            {
                AppUserId = Guid.Parse("b54a920d-0766-4532-8dd8-d98b1df79b37"),
                BarId = Guid.Parse("5416ceee-839d-43e3-bb85-b292976c353e"),
                Score = 6,
            };

            using var assertContext = new CMContext(options);
            {
                var sut = new RatingServices(assertContext, mockCocktailMapper.Object, mockBarMapper.Object);

                await Assert.ThrowsExceptionAsync<NullReferenceException>(() => sut.RateBarAsync(input));
            }
        }

        [TestMethod]
        public async Task ThrowArgumentNullException_When_InputIsNull()
        {
            var options = Utility.GetOptions(nameof(ThrowArgumentNullException_When_InputIsNull));
            await Utility.ArrangeContextAsync(options);

            var mockBarMapper = new Mock<IBarMapper>();
            var mockCocktailMapper = new Mock<ICocktailMapper>();

            using var assertContext = new CMContext(options);
            {
                var sut = new RatingServices(assertContext, mockCocktailMapper.Object, mockBarMapper.Object);

                await Assert.ThrowsExceptionAsync<ArgumentNullException>(() => sut.RateBarAsync(null));
            }
        }

        [TestMethod]
        public async Task ReturnDtoWhen_InputValid()
        {
            //Arrange

            var options = Utility.GetOptions(nameof(ReturnDtoWhen_InputValid));
            await Utility.ArrangeContextAsync(options);

            var mockCocktailMapper = new Mock<ICocktailMapper>();
            var mockBarMapper = new Mock<IBarMapper>();
            mockBarMapper.Setup(x => x.CreateBarDTO(It.IsAny<Bar>()))
                      .Returns<Bar>(bar => new BarDTO
                      {
                          Id = bar.Id,
                          Name = bar.Name,
                          AverageRating = bar.AverageRating
                      });

            var existingRating = new BarRating
            {
                AppUserId = Guid.Parse("b8a3552e-f509-42f0-bed9-7c29c3dfc6b6"),
                BarId = Guid.Parse("9bdbf5e7-ad83-415c-b359-9ff5e2f0dedd"),
                Score = 3,
            };

            var input = new BarRatingDTO
            {
                AppUserId = Guid.Parse("b54a920d-0766-4532-8dd8-d98b1df79b37"),
                BarId = Guid.Parse("9bdbf5e7-ad83-415c-b359-9ff5e2f0dedd"),
                Score = 5,
            };

            using (var arrangeContext = new CMContext(options))
            {
                await arrangeContext.BarRatings.AddAsync(existingRating);
                await arrangeContext.SaveChangesAsync();
            }

            //Act/Assert

            using (var assertContext = new CMContext(options))
            {
                var sut = new RatingServices(assertContext, mockCocktailMapper.Object, mockBarMapper.Object);

                var result = await sut.RateBarAsync(input);
                var bar = await assertContext.Bars.FirstOrDefaultAsync(bar => bar.Id == Guid.Parse("9bdbf5e7-ad83-415c-b359-9ff5e2f0dedd"));
                Assert.AreEqual(4, bar.AverageRating);
            }
        }

    }
}