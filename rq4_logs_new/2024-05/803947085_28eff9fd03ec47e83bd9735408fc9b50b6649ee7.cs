using Azure.Storage.Blobs;
using Event_Planning_System.Helpers;
using Event_Planning_System.IServices;
using Microsoft.Extensions.Options;

namespace Event_Planning_System.Services
{
    public class BlobService : IBlobServices
    {


        private readonly BlobServiceClient blobServiceClient;
        private readonly BlobContainerClient blobContainerClient;
        private readonly AzureStorage AzureConnection;

        public BlobService(IConfiguration configuration, IOptions<AzureStorage> azureConnection)
        {
            AzureConnection = azureConnection.Value;
            blobServiceClient = new BlobServiceClient(AzureConnection.StorageConnectionString);
            blobContainerClient = blobServiceClient.GetBlobContainerClient(AzureConnection.ContainerName);
        }

        public async Task<string> AddingImage(IFormFile image)
        {
            if (image == null)
            {
                return null;
            }
            try
            {
                await blobContainerClient.CreateIfNotExistsAsync();
                var blobName = $"{Guid.NewGuid()}{Path.GetExtension(image.FileName)}";
                var blobClient = blobContainerClient.GetBlobClient(blobName);
                await blobClient.UploadAsync(image.OpenReadStream(), true);

                var imageUrl = blobClient.Uri.ToString();
                return imageUrl;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public async Task<bool> RemoveImage(string imagename)
        {
            if (imagename == null)
            {
                return false;
            }
            try
            {
                var uri = new Uri(imagename);
                var blobname = Path.GetFileName(uri.LocalPath);
                var blobServiceClient = new BlobServiceClient(AzureConnection.StorageConnectionString);

                var blobContainerClient = blobServiceClient.GetBlobContainerClient(AzureConnection.ContainerName);

                // Get the blob client
                var blobClient = blobContainerClient.GetBlobClient(blobname);

                // Delete the blob
                var response = await blobClient.DeleteIfExistsAsync();

                return response != null ? true : false;
            }
            catch (Exception ex)
            {
                throw;
            }

        }

        public async Task<string> UpdateImage(string oldimage, IFormFile image)
        {
            try
            {
                if (image == null)
                {
                    return oldimage;
                }
                var result = await RemoveImage(oldimage);

                return await AddingImage(image);

            }
            catch (Exception ex)
            {
                throw;
            }
        }
    }
}