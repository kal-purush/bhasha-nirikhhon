using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using CaptoneProject_IOTS_BOs;
using CaptoneProject_IOTS_BOs.DTO.FileDTO;
using CaptoneProject_IOTS_Service.ResponseService;
using CaptoneProject_IOTS_Service.Services.Interface;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static CaptoneProject_IOTS_BOs.DTO.FileDTO.FileDTO;

namespace CaptoneProject_IOTS_Service.Services.Implement
{
    public class BlobService : IBlobService
    {
        private readonly BlobContainerClient fileContainer;
        private readonly string accountStorage = "iottradingsystemstorage";
        private readonly string key = "TArKZqfxiK+9lUFyIry6+yfh2PjryjW2r0v0lYEgYqikZGjFsIkrNw8jEoYNjByGJJ0zFPkkYZ50+AStgEIMtQ==";

        public BlobService(
            //string accountStorage, string key
        )
        {
            //this.accountStorage = accountStorage;
            //this.key = key;

            var credential = new StorageSharedKeyCredential(this.accountStorage, key);
            var blobUri = $"https://{this.accountStorage}.blob.core.windows.net";
            var blobServiceClient = new BlobServiceClient(new Uri(blobUri), credential);

            fileContainer = blobServiceClient.GetBlobContainerClient("iotstoragevideo");
        }

        public async Task<GenericResponseDTO<FileResponseDTO>> UploadVideoAsync(IFormFile file)
        {
            FileResponseDTO response = new FileResponseDTO();
            string videoName = Guid.NewGuid().ToString() + ".mp4";

            BlobClient client = fileContainer.GetBlobClient(videoName);

            var options = new BlobUploadOptions
            {
                HttpHeaders = new BlobHttpHeaders
                {
                    ContentType = "video/mp4"
                }
            };

            try
            {
                await using (Stream? data = file.OpenReadStream())
                {
                    await client.UploadAsync(data, options);
                }
            } catch
            {
                ResponseService<FileResponseDTO>.BadRequest("Error Upload. Please try again");
            }

            return ResponseService<FileResponseDTO>.OK(new FileResponseDTO
            {
                Id = client.Uri.AbsoluteUri,
                FileName = videoName
            });
        }
    }
}