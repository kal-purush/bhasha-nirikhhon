using Application.Contract.Services;
using Microsoft.AspNetCore.Http;

namespace Application.ServicesImpl
{
    public class FileStorageService : IFileStorageService
    {
        private readonly string _webRootPath;
        private readonly IHttpContextAccessor _httpContextAccessor;

        public FileStorageService(IHttpContextAccessor httpContextAccessor)
        {
            _webRootPath = "wwwroot";
            _httpContextAccessor = httpContextAccessor;
        }

        public async Task<string> SaveFileAsync(IFormFile file, string category)
        {
            var folderPath = Path.Combine(_webRootPath, "images", category);

            if (!Directory.Exists(folderPath))
                Directory.CreateDirectory(folderPath);

            var fileName = $"{Guid.NewGuid()}{Path.GetExtension(file.FileName)}";
            var fullPath = Path.Combine(folderPath, fileName);

            using (var stream = new FileStream(fullPath, FileMode.Create))
            {
                await file.CopyToAsync(stream);
            }

            var request = _httpContextAccessor.HttpContext.Request;
            var baseUrl = $"{request.Scheme}://{request.Host}";
            var url = $"{baseUrl}/images/{category}/{fileName}";

            return url;
        }

        public async Task<bool> DeleteFileAsync(string fileUrl)
        {
            var uri = new Uri(fileUrl);
            var relativePath = uri.AbsolutePath.TrimStart('/').Replace('/', Path.DirectorySeparatorChar);
            var fullPath = Path.Combine(_webRootPath, relativePath);

            if (File.Exists(fullPath))
            {
                File.Delete(fullPath);
                return true;
            }

            return false;
        }
    }
}