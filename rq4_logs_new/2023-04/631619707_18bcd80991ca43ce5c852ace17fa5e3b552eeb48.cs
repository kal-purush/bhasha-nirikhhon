using Autofac.Extras.Moq;
using MockQueryable.Moq;
using Moq;
using NUnit.Framework;
using PocketForzaHorizonCommunity.Back.Database.Entities.Guides;
using PocketForzaHorizonCommunity.Back.Database.Repos.Interfaces;
using PocketForzaHorizonCommunity.Back.Services.Services;
using PocketForzaHorizonCommunity.Back.Services.Utilities.Interfaces;
namespace PocketForzaHorizonCommunity.Back.Services.Tests.Services;

[TestFixture]
public class DesignServiceTests
{
    [Test]
    public async Task CreateAsync_Design_Should_Have_Thumbnail()
    {
        using var mock = AutoMock.GetLoose();
        var entity = new Design
        {
            Title = "Design",
            DesignOptions = new DesignOptions
            {
                Description = "Description",
            }
        };
        var path = @"path\to\thumbnail";

        mock.Mock<IDesignRepository>()
            .Setup(x => x.CreateAsync(entity));
        mock.Mock<IDesignRepository>()
            .Setup(x => x.SaveAsync());

        mock.Mock<IImageManager>()
            .Setup(x => x.SaveDesignThumbnail(null, entity.Id))
            .Returns(Task.Run(() => path));

        var designService = mock.Create<DesignService>();

        var actual = await designService.CreateAsync(entity, null);

        Assert.IsTrue(path.Equals(actual.DesignOptions.ThumbnailPath));
    }

    [Test]
    public async Task DeleteAsync_Should_Delete_All_Images()
    {
        var entity = new Design
        {
            DesignOptions = new DesignOptions
            {
                ThumbnailPath = @"thumbnail\path",
                Gallery = new List<GalleryImage>(),
            }
        };

        using var mock = AutoMock.GetLoose();
        mock.Mock<IDesignRepository>()
            .Setup(x => x.GetById(entity.Id))
            .Returns(new List<Design> { entity }.BuildMock());
        mock.Mock<IDesignRepository>()
            .Setup(x => x.Delete(entity));
        mock.Mock<IDesignRepository>()
            .Setup(x => x.SaveAsync());
        mock.Mock<IImageManager>()
            .Setup(x => x.DeleteDesignImages(entity.DesignOptions.ThumbnailPath, entity.DesignOptions.Gallery.ToList()));
        mock.Mock<IGalleryRepository>()
            .Setup(x => x.SaveAsync());

        var desginService = mock.Create<DesignService>();

        await desginService.DeleteAsync(entity.Id);

        mock.Mock<IImageManager>()
            .Verify(x => x.DeleteDesignImages(
                entity.DesignOptions.ThumbnailPath, entity.DesignOptions.Gallery.ToList()),
                Times.Once);
    }
}