using CloudinaryDotNet.Actions;

namespace API.Interfaces;

public interface IPhotoServices
{
    /// <summary>
    /// Get a file and upload on cloudinary
    /// </summary>
    /// <param name="file"></param>
    /// <returns>UploadResult</returns>
    Task<ImageUploadResult> AddPhotoAsync(IFormFile file);
    
    /// <summary>
    /// Use photo public id anf delete photo
    /// </summary>
    /// <param name="publicId"></param>
    /// <returns>DeleteResult</returns>
    Task<DeletionResult> DeletePhotoAsync(string publicId);
}