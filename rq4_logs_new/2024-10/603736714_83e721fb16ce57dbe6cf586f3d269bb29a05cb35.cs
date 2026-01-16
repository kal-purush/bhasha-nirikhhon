using Registration.DomainCore.CloudAbstration;
using System.ComponentModel.DataAnnotations;

namespace HandlersTest.Builders.CloudHandlers;
internal class AWSBucketS3Test : IImageStorage
{
    public HashSet<string> AllowImageTypes { get; } = new HashSet<string>(new[] { "jpg", "jpeg" });
    [Required]
    public string StorageName { get; set; } = null!;
    [Required]
    public string FileName { get; set; } = null!;
    [Required]
    public string ImageType { get; set; } = null!;
    [Required]
    public string Base64Image { get; set; } = null!;
    public string? MessageLog { get; set; }
    public string ImagePath { get; set; } = null!;

    public Task<bool> SaveImage()
    {
        return Task.FromResult(true);
    }
}