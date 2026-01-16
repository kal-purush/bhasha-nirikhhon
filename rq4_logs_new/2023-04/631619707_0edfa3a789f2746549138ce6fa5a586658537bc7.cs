using Autofac.Extras.Moq;
using MockQueryable.Moq;
using Moq;
using NUnit.Framework;
using PocketForzaHorizonCommunity.Back.Database.Entities.CarEntities;
using PocketForzaHorizonCommunity.Back.Database.Repos.Interfaces;
using PocketForzaHorizonCommunity.Back.Services.Services;
using PocketForzaHorizonCommunity.Back.Services.Utilities.Interfaces;

namespace PocketForzaHorizonCommunity.Back.Services.Tests.Services;

[TestFixture]
public class CarServiceTests
{
    [Test]
    public async Task CreateAsync_Car_Should_Have_Thumbnail()
    {
        var entity = new Car();

        var thumbnailPath = @"thumbnail\path";
        using var mock = AutoMock.GetLoose();
        mock.Mock<ICarRepository>()
            .Setup(x => x.CreateAsync(entity));
        mock.Mock<ICarRepository>()
            .Setup(x => x.SaveAsync());
        mock.Mock<IImageManager>()
            .Setup(x => x.SaveCarThumbnail(null, entity.Id))
            .Returns(Task.Run(() => thumbnailPath));

        var carService = mock.Create<CarService>();

        await carService.CreateAsync(entity, null);

        Assert.IsTrue(entity.ImagePath.Equals(thumbnailPath));
    }

    [Test]
    public async Task DeleteAsync_Should_Delete_Thumbnail()
    {
        var entity = new Car
        {
            ImagePath = @"thumbnail\path",
        };

        using var mock = AutoMock.GetLoose();
        mock.Mock<ICarRepository>()
            .Setup(x => x.GetById(entity.Id))
            .Returns(new List<Car> { entity }.BuildMock());
        mock.Mock<ICarRepository>()
            .Setup(x => x.Delete(entity));
        mock.Mock<ICarRepository>()
            .Setup(x => x.SaveAsync());
        mock.Mock<IImageManager>()
            .Setup(x => x.DeleteCarThumbnail(entity.ImagePath));

        var carService = mock.Create<CarService>();
        await carService.DeleteAsync(entity.Id);

        mock.Mock<IImageManager>()
            .Verify(x => x.DeleteCarThumbnail(entity.ImagePath), Times.Once);
    }
}