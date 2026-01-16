using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.AspNetCore.Http;

namespace Unitee.Helpers;

public record FluentBlobStorage
{
    public string ConnectionString { get; set; }
    public string ContainerName { get; set; }
    public string FileName { get; set; }
    public BlobHttpHeaders Headers { get; set; } = new BlobHttpHeaders();

    private void AssertGuards()
    {
        if (ConnectionString == null)
        {
            throw new ArgumentNullException(nameof(ConnectionString));
        }

        if (ContainerName == null)
        {
            throw new ArgumentNullException(nameof(ContainerName));
        }

        if (FileName == null)
        {
            throw new ArgumentNullException(nameof(FileName));
        }
    }

    public FluentBlobStorage WithConnectionString(string connectionString)
    {
        return this with { ConnectionString = connectionString };
    }

    public FluentBlobStorage WithContainerName(string containerName)
    {
        return this with { ContainerName = containerName };
    }

    public FluentBlobStorage WithFileName(string fileName)
    {
        return this with { FileName = fileName };
    }

    public FluentBlobStorage WithContentType(string contentType)
    {
        return this with
        {
            Headers = new BlobHttpHeaders()
            {
                ContentType = contentType,
                CacheControl = Headers.CacheControl,
                ContentDisposition = Headers.ContentDisposition,
                ContentEncoding = Headers.ContentEncoding,
                ContentLanguage = Headers.ContentLanguage,
            }
        };
    }

    public async Task<(Uri, Azure.Response<BlobContentInfo>)> UploadAsync(IFormFile f)
    {
        AssertGuards();

        BlobServiceClient blobServiceClient = new(ConnectionString);
        BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(ContainerName);
        BlobClient blobClient = containerClient.GetBlobClient(FileName);

        if (Headers.ContentType == null)
        {
            Headers.ContentType = f.ContentType;
        }

        var res = await blobClient.UploadAsync(f.OpenReadStream(), Headers);

        return (blobClient.Uri, res);
    }

    public async Task<(Uri, Azure.Response<BlobContentInfo>)> UploadAsync(Stream s)
    {
        AssertGuards();

        BlobServiceClient blobServiceClient = new(ConnectionString);
        BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(ContainerName);
        BlobClient blobClient = containerClient.GetBlobClient(FileName);

        var res = await blobClient.UploadAsync(s, Headers);

        return (blobClient.Uri, res);
    }

    public (Stream, BlobProperties) OpenRead()
    {
        AssertGuards();

        BlobServiceClient blobServiceClient = new(ConnectionString);
        BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(ContainerName);
        BlobClient blobClient = containerClient.GetBlobClient(FileName);

        return (blobClient.OpenRead(), blobClient.GetProperties()?.Value);
    }
}