using Azure.Storage.Blobs;
using EST.MIT.InvoiceImporter.Function.Interfaces;
using System.Diagnostics.CodeAnalysis;

namespace EST.MIT.InvoiceImporter.Function.Services;

[ExcludeFromCodeCoverage]
public class AzureBlobService : IAzureBlobService
{
    private readonly BlobServiceClient _blobServiceClient;


    public AzureBlobService(BlobServiceClient blobServiceClient)
    {
        _blobServiceClient = blobServiceClient;
    }

    public BlobServiceClient? GetBlobServiceClient()
    {
        return _blobServiceClient;
    }
}