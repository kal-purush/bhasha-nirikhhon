using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace DoctorAppointment_Android
{
    public class AzureStorageManager
    {
        private readonly string _storageConnectionString;

        public AzureStorageManager(string storageConnectionString)
        {
            _storageConnectionString = storageConnectionString;
        }

        public void UploadDatabaseToAzureStorage(string localDatabaseFilePath, string destinationBlobName)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(_storageConnectionString);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
           CloudBlobContainer container = blobClient.GetContainerReference("your-container-name");

            CloudBlockBlob blockBlob = container.GetBlockBlobReference(destinationBlobName);

            using (var fileStream = File.OpenRead(localDatabaseFilePath))
            {
                blockBlob.UploadFromStreamAsync(fileStream);
            }
        }
    }
}