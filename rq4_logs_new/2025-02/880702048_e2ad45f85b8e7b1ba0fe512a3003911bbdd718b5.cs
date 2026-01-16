using Amazon.S3;
using Amazon.S3.Model;
using FilesService.Application.Interfaces;
using FilesService.Core.Requests;
using FilesService.Core.Responses;
using Microsoft.AspNetCore.Mvc;

namespace FilesService.Application.Features.AmazonS3.MultipartUpload;

public static class CompleteMultipartUpload
{
    public sealed class Endpoint : IEndpoint
    {
        public void MapEndpoint(IEndpointRouteBuilder app)
        {
            app.MapPost("amazon/files/{key}/complete-multipart/presigned", Handler);
        }
    }
    private static async Task<IResult> Handler(
        [FromRoute] string key,
        [FromBody] CompleteMultipartRequest request,
        IAmazonS3 s3Client,
        //MongoDbRepository repository,
        CancellationToken cancellationToken)
    {
        try
        {
            /*var fileId = Guid.NewGuid();

            //job проверки файла в mongo и s3
            var enqueueAt = TimeSpan.FromHours(24);
            var jobId = BackgroundJob.Schedule<ConfirmConsistencyJob>(j => j.Execute(fileId, key, request.BucketName, ct), enqueueAt);*/

            var presignedRequest = new CompleteMultipartUploadRequest
            {
                BucketName = request.BucketName,
                Key = key,
                UploadId = request.UploadId,
                PartETags = request.Parts
                    .Select(p => new PartETag(p.PartNumber, p.ETag)).ToList()
            };

            var response = await s3Client.CompleteMultipartUploadAsync(
                presignedRequest, cancellationToken);

            /*var metaDataRequest = new GetObjectMetadataRequest
            {
                BucketName = request.BucketName,
                Key = key,
            };
            var metaData = await s3Client.GetObjectMetadataAsync(metaDataRequest, cancellationToken: ct);

            FileData fileData = new FileData
            {
                Id = fileId,
                StoragePath = key,
                UploadDate = DateTime.UtcNow,
                FileSize = metaData.Headers.ContentLength,
                ContentType = metaData.Headers.ContentType,
            };

            await repository.Add(fileData, ct);

            BackgroundJob.Delete(jobId);

            FileLocationResponse fileLocation = new FileLocationResponse(key, response.Location);*/
            
            return Results.Ok(new
            {
                key, location = response.Location
            });
        }
        catch (AmazonS3Exception ex)
        {
            return Results.BadRequest($"S3: complete multipart upload failed: \r\t\n{ex.Message}");
        }
    }
}