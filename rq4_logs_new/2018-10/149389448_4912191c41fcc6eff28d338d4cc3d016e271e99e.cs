using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MAJServices.Controllers
{
    [Route("api/files")]
    public class FileController : Controller
    {
        [HttpPost("upload")]
        public async Task<IActionResult> SaveFile(IFormFile file)
        {
            //set the connections string
            string storageConnectionString = Environment.GetEnvironmentVariable("AzureBlobStorage");
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            
            // Create the CloudBlobClient that represents the Blob storage endpoint for the storage account.
            CloudBlobClient BlobClient = storageAccount.CreateCloudBlobClient();

            // Create a reference to the container 
            CloudBlobContainer BlobContainer = BlobClient.GetContainerReference("photos");

            //Get a reference to the blob
            CloudBlockBlob blockBlob = BlobContainer.GetBlockBlobReference(file.FileName);

            //Create or overwrite the blob with the contents of a local file
            using(var filestream = file.OpenReadStream())
            {
                await blockBlob.UploadFromStreamAsync(filestream);
            }
            return Json(new
            {
                name = blockBlob.Name,
                uri = blockBlob.Uri,
                size = blockBlob.Properties.Length
            });
        }
    }
}