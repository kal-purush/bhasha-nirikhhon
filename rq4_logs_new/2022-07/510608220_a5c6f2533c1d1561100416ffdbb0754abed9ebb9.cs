using System;
using System.Configuration;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace ExtractCompressedFilesAndProcessCSV
{
    [StorageAccount("BlobConnectionString")]
    public class ProcessCompressedFiles
    {
        [FunctionName("ProcessCompressedFiles")]
        public async Task Run(
            [BlobTrigger("compressed-files/{inputBlobName}")]Stream inputBlob
            , string inputBlobName
            , ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{inputBlobName} \n Size: {inputBlob.Length} Bytes");

            if (Path.GetExtension(inputBlobName)?.ToLower() == ".zip")
            {
                string connectionString = Environment.GetEnvironmentVariable("BlobConnectionString");
                string containerName = "uncompressed-files";
                
                // Get a reference to a container named "sample-container" and then create it
                BlobContainerClient outputContainer = new BlobContainerClient(connectionString, containerName);
                outputContainer.CreateIfNotExists();

                var archive = new ZipArchive(inputBlob);
                foreach (var entry in archive.Entries)
                {
                    // Get a reference to a blob named "sample-file" in a container named "sample-container"
                    BlobClient blockBlob = outputContainer.GetBlobClient($"{inputBlobName}/{entry.FullName}");

                    using (var fileStream = entry.Open())
                    {
                        if (entry.Length > 0)
                        {
                            log.LogInformation($"Extracting - {entry.FullName} to - {blockBlob.Name}");
                            // Upload local file
                            await blockBlob.UploadAsync(fileStream);
                        }
                    }
                }
            }
            else
            {
                log.LogInformation("Not a zip file. Ignoring");
            }
        }
    }
}