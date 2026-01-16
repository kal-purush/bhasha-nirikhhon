using CM.Web.Providers.Contracts;
using Microsoft.AspNetCore.Http;
using System.IO;
using System.Threading.Tasks;

namespace CM.Web.Providers
{
    public class AppStorageProvider : IStorageProvider
    {
        public async Task StoreImageAsync(string path, IFormFile file)
        {
            await file.CopyToAsync(new FileStream(path, FileMode.Create));
        }

        public void DeleteImage(string path)
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
    }
}