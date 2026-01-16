using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using PocketForzaHorizonCommunity.Back.Database.Entities.Guides;
using PocketForzaHorizonCommunity.Back.Services.Utilities.Interfaces;

namespace PocketForzaHorizonCommunity.Back.Services.Utilities;

public class ImageManager : IImageManager
{
    private IConfiguration _config;
    public ImageManager(IConfiguration config)
    {
        _config = config;
    }
    public async Task<string> SaveCarThumbnail(IFormFile image, Guid carId)
    {
        var applicationDirectory = AppDomain.CurrentDomain.BaseDirectory;
        var path = Path.Combine(_config["Images:Cars"], carId.ToString());
        using (var stream = new FileStream(path, FileMode.Create))
        {
            await image.CopyToAsync(stream);
        }

        return path;
    }

    public async Task<string> SaveDesignThumbnail(IFormFile image, Guid designId)
    {
        var applicationDirectory = AppDomain.CurrentDomain.BaseDirectory;
        var path = Path.Combine(_config["Images:Designs"], designId.ToString(), "_thumbnail");
        using (var stream = new FileStream(path, FileMode.Create))
        {
            await image.CopyToAsync(stream);
        }

        return path;
    }

    public async Task<List<string>> SaveDesignGallery(List<IFormFile> images, Guid designId)
    {
        var applicationDirectory = AppDomain.CurrentDomain.BaseDirectory;
        var galleryPath = new List<string>();

        for (var i = 0; i < images.Count, i++)
        {
            var path = Path.Combine(_config["Images:Designs"], designId.ToString(), $"_{i}");
            using (var stream = new FileStream(path, FileMode.Create))
            {
                await images[i].CopyToAsync(stream);
            }
        }

        return galleryPath;
    }

    public void DeleteCarThumbnail(string thumbnailPath)
    {
        if (File.Exists(thumbnailPath)) File.Delete(thumbnailPath);
    }

    public void DeleteDesignImages(string thumbnailPath, List<GalleryImage>? gallery)
    {
        if (File.Exists(thumbnailPath)) File.Delete(thumbnailPath);

        if (gallery == null) return;

        foreach (var image in gallery)
        {
            if (File.Exists(image.ImagePath)) File.Delete(image.ImagePath);
        }
    }
}