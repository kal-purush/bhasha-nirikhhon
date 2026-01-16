using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.Formats.Jpeg;
using SixLabors.ImageSharp.Processing;
using SixLabors.ImageSharp.Processing.Transforms;
namespace ServerlessImageManagement
{
    public static class CreateThumbnail
    {
        [FunctionName("CreateThumbnail")]
        public static async Task Run([QueueTrigger("thumbnails", Connection = "ImageStorageAccount")] string thumbnailPath,
          [Blob("{queueTrigger}", Connection = "ImageStorageAccount")] ICloudBlob resizedPhotoCloudBlob, TraceWriter log)
        {
            try
            {
                var imagePath = Utils.GetImagePathFromThumbnail(thumbnailPath);
                var photoBlob = await Utils.BlobClient.GetBlobReferenceFromServerAsync(new Uri(imagePath, UriKind.Absolute));
                var photoStream = await photoBlob.OpenReadAsync(AccessCondition.GenerateEmptyCondition(),
                  new BlobRequestOptions(), new OperationContext());

                var image = Image.Load(photoStream);
                image.Mutate(e => e.Resize(140, 140));
                var resizedPhotoStream = new MemoryStream();
                image.Save(resizedPhotoStream, new JpegEncoder());
                resizedPhotoStream.Seek(0, SeekOrigin.Begin);
                await resizedPhotoCloudBlob.UploadFromStreamAsync(resizedPhotoStream);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

    }
}